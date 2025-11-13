from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import sheets_gateway


class _FakeRequest:
    def __init__(self, callback):
        self._callback = callback

    def execute(self):
        return self._callback()


class _FakeValues:
    def __init__(self, service: "_FakeService") -> None:
        self._service = service

    def get(self, spreadsheetId: str, range: str):  # noqa: N802 - API compatibility
        return _FakeRequest(lambda: self._service._handle_get(range))

    def batchUpdate(self, spreadsheetId: str, body: Dict[str, Any]):  # noqa: N802 - API compatibility
        return _FakeRequest(lambda: self._service._handle_batch_update(body))


class _FakeSpreadsheets:
    def __init__(self, service: "_FakeService") -> None:
        self._service = service

    def values(self) -> _FakeValues:  # noqa: D401 - API compatibility
        return _FakeValues(self._service)


class _FakeService:
    def __init__(self, rows: Iterable[List[Any]] | None = None) -> None:
        self.sheet_rows: List[List[Any]] = [list(row) for row in rows] if rows else []
        self.batch_requests: List[Dict[str, Any]] = []

    def spreadsheets(self) -> _FakeSpreadsheets:  # noqa: D401 - API compatibility
        return _FakeSpreadsheets(self)

    # Internal helpers -------------------------------------------------
    def _handle_get(self, range_spec: str) -> Dict[str, Any]:
        _sheet, cell_range = self._split_range(range_spec)
        if cell_range == "1:1":
            header = self.sheet_rows[0:1]
            return {"values": header}

        match = re.match(r"[A-Z]+(\d+):[A-Z]+(\d+)", cell_range)
        if not match:
            return {"values": []}
        start = int(match.group(1)) - 1
        return {"values": [list(row) for row in self.sheet_rows[start:]]}

    def _handle_batch_update(self, body: Dict[str, Any]) -> Dict[str, Any]:
        self.batch_requests.append(body)
        ranges = body.get("data", [])
        if not ranges:
            return {}

        max_row = 0
        parsed_updates: List[tuple[int, List[List[Any]]]] = []
        for entry in ranges:
            range_spec = entry.get("range", "")
            _sheet, cell_range = self._split_range(range_spec)
            match = re.match(r"([A-Z]+)(\d+):([A-Z]+)(\d+)", cell_range)
            if not match:
                continue
            start_row = int(match.group(2)) - 1
            end_row = int(match.group(4))
            max_row = max(max_row, end_row)
            values = [list(row) for row in entry.get("values", [])]
            parsed_updates.append((start_row, values))

        if max_row == 0:
            return {}

        width = 0
        for _, values in parsed_updates:
            if values:
                width = len(values[0])
                break
        if width == 0:
            width = len(self.sheet_rows[0]) if self.sheet_rows else len(sheets_gateway.REQUIRED_HEADERS)

        new_matrix: List[List[Any]] = [["" for _ in range(width)] for _ in range(max_row)]
        for start_row, values in parsed_updates:
            for offset, row in enumerate(values):
                new_matrix[start_row + offset] = list(row)

        self.sheet_rows = new_matrix
        return {}

    @staticmethod
    def _split_range(range_spec: str) -> tuple[str, str]:
        if "!" not in range_spec:
            return "", range_spec
        sheet, cell_range = range_spec.split("!", 1)
        sheet = sheet.strip()
        if sheet.startswith("'") and sheet.endswith("'") and len(sheet) >= 2:
            sheet = sheet[1:-1].replace("''", "'")
        return sheet, cell_range


def _row(**overrides: Any) -> Dict[str, Any]:
    row = {header: None for header in sheets_gateway.REQUIRED_HEADERS}
    row.update(overrides)
    return row


def test_get_rows_converts_numeric_and_blank_cells() -> None:
    header = list(sheets_gateway.REQUIRED_HEADERS)
    data = [
        [
            "R1",
            "Modern Collection",
            "Heritage",
            "Red",
            "Cream",
            "8x10",
            "96x120",
            "120.5",
            "Handmade",
            "Rectangle",
            "Classic",
            "rug.jpg",
            "Turkey",
            "1999.5",
            "1899.5",
            "2499.5",
            "900",
            "Wool",
            "2024-01-02T11:00:00Z",
            "TRUE",
        ]
    ]
    service = _FakeService([header] + data)

    rows = sheets_gateway.get_rows(service=service)

    assert rows == [
        {
            "RugNo": "R1",
            "Collection": "Modern Collection",
            "Design": "Heritage",
            "Ground": "Red",
            "Border": "Cream",
            "ASize": "8x10",
            "SSize": "96x120",
            "Area": 120.5,
            "Type": "Handmade",
            "Shape": "Rectangle",
            "Style": "Classic",
            "ImageFileName": "rug.jpg",
            "Origin": "Turkey",
            "Retail": 1999.5,
            "SP": 1899.5,
            "MSRP": 2499.5,
            "Cost": 900.0,
            "Content": "Wool",
            "LastUpdated": "2024-01-02T11:00:00Z",
            "Deleted": True,
        }
    ]


def test_upsert_rows_writes_headers_and_chunks_batches() -> None:
    service = _FakeService()
    rows = [
        _row(RugNo=f"R{index}", LastUpdated="2024-01-01T00:00:00Z", Deleted=False)
        for index in range(1205)
    ]

    sheets_gateway.upsert_rows(rows, service=service)

    assert service.sheet_rows[0] == list(sheets_gateway.REQUIRED_HEADERS)
    assert len(service.batch_requests) >= 1
    body = service.batch_requests[-1]
    assert all(len(entry.get("values", [])) <= 50 for entry in body["data"])  # 1000 cells limit


def test_upsert_rows_prefers_rugno_for_matching() -> None:
    header = list(sheets_gateway.REQUIRED_HEADERS)
    base_row = _row(
        RugNo="R-1",
        Collection="Vintage",
        Design="Old Title",
        Ground="Blue",
        Border="Ivory",
        ASize="8x10",
        SSize="96x120",
        Area="120",
        Type="Handmade",
        Shape="Rectangle",
        Style="Classic",
        ImageFileName="old.jpg",
        Origin="Turkey",
        Retail="1999",
        SP="1899",
        MSRP="2499",
        Cost="900",
        Content="Wool",
        LastUpdated="2024-01-01T00:00:00Z",
        Deleted=False,
    )
    existing = [header, [base_row.get(column) for column in header]]
    service = _FakeService(existing)

    sheets_gateway.upsert_rows(
        [
            {
                "RugNo": "R-1",
                "Design": "Updated",
                "LastUpdated": "2024-02-01T12:00:00Z",
            }
        ],
        service=service,
    )

    design_index = header.index("Design")
    updated_timestamp_index = header.index("LastUpdated")
    assert service.sheet_rows[1][design_index] == "Updated"
    assert service.sheet_rows[1][updated_timestamp_index] == "2024-02-01T12:00:00Z"


def test_delete_rows_removes_matching_entries() -> None:
    header = list(sheets_gateway.REQUIRED_HEADERS)
    first = _row(RugNo="R-1", LastUpdated="2024-01-01T00:00:00Z", Deleted=False)
    second = _row(RugNo="R-2", LastUpdated="2024-01-01T00:00:00Z", Deleted=False)
    data = [header, [first.get(column) for column in header], [second.get(column) for column in header]]
    service = _FakeService(data)

    sheets_gateway.delete_rows(["R-1"], service=service)

    assert len(service.sheet_rows) == 2  # header + remaining row
    assert service.sheet_rows[1][0] == "R-2"


