from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pytest

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
        if range_spec.endswith("1:1"):
            header = self.sheet_rows[0:1]
            return {"values": header}

        match = re.match(rf"{sheets_gateway.SHEET_NAME}![A-Z]+(\d+):[A-Z]+", range_spec)
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
            match = re.match(rf"{sheets_gateway.SHEET_NAME}!([A-Z]+)(\d+):([A-Z]+)(\d+)", range_spec)
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


def _row(**overrides: Any) -> Dict[str, Any]:
    row = {header: None for header in sheets_gateway.REQUIRED_HEADERS}
    row.update(overrides)
    return row


def test_get_rows_converts_numeric_and_blank_cells() -> None:
    header = list(sheets_gateway.REQUIRED_HEADERS)
    data = [
        [
            "R1",
            "SKU-1",
            "Modern Rug",
            "Heritage",
            "Classic",
            "Turkey",
            "Wool",
            "Red",
            "8x10",
            "120",
            "180",
            "1999.5",
            "2499.5",
            "900",
            "Warehouse",
            "Available",
            "2024-01-01T10:00:00Z",
            "2024-01-02T11:00:00Z",
            "",
            "123",
            "",
        ]
    ]
    service = _FakeService([header] + data)

    rows = sheets_gateway.get_rows(service=service)

    assert rows == [
        {
            "RugNo": "R1",
            "SKU": "SKU-1",
            "Title": "Modern Rug",
            "Collection": "Heritage",
            "Style": "Classic",
            "Origin": "Turkey",
            "Material": "Wool",
            "Color": "Red",
            "Size": "8x10",
            "WidthIn": 120.0,
            "LengthIn": 180.0,
            "Price": 1999.5,
            "MSRP": 2499.5,
            "Cost": 900.0,
            "Location": "Warehouse",
            "Status": "Available",
            "CreatedAt": "2024-01-01T10:00:00Z",
            "UpdatedAt": "2024-01-02T11:00:00Z",
            "SoldAt": None,
            "CustomerId": 123,
            "Notes": None,
        }
    ]


def test_upsert_rows_writes_headers_and_chunks_batches() -> None:
    service = _FakeService()
    rows = [_row(RugNo=f"R{index}", Status="Available") for index in range(1205)]

    sheets_gateway.upsert_rows(rows, service=service)

    assert service.sheet_rows[0] == list(sheets_gateway.REQUIRED_HEADERS)
    assert len(service.batch_requests) >= 1
    body = service.batch_requests[-1]
    assert len(body["data"]) == 2  # 1206 rows (header + data) should be split across two chunks


def test_upsert_rows_prefers_rugno_for_matching() -> None:
    header = list(sheets_gateway.REQUIRED_HEADERS)
    existing = [
        header,
        [
            "R-1",
            "SKU-1",
            "Old Title",
            "Collect",
            "Style",
            "Origin",
            "Material",
            "Color",
            "Size",
            "10",
            "12",
            "100",
            "200",
            "50",
            "A1",
            "Available",
            "2024-01-01T00:00:00Z",
            "2024-01-01T00:00:00Z",
            "",
            "",
            "",
        ],
    ]
    service = _FakeService(existing)

    sheets_gateway.upsert_rows(
        [
            {
                "RugNo": "R-1",
                "SKU": "SKU-NEW",
                "Title": "Updated",
                "Status": "Reserved",
                "UpdatedAt": "2024-02-01T12:00:00Z",
            }
        ],
        service=service,
    )

    assert service.sheet_rows[1][2] == "Updated"  # Title column
    assert service.sheet_rows[1][15] == "Reserved"


def test_delete_rows_removes_matching_entries() -> None:
    header = list(sheets_gateway.REQUIRED_HEADERS)
    data = [
        header,
        list(_row(RugNo="R-1", Status="Available").values()),
        list(_row(SKU="SKU-2", Status="Sold").values()),
    ]
    service = _FakeService(data)

    sheets_gateway.delete_rows(["R-1"], service=service)

    assert len(service.sheet_rows) == 2  # header + remaining row
    assert service.sheet_rows[1][0] == ""  # RugNo cleared for remaining row


def test_upsert_rows_rejects_invalid_status() -> None:
    service = _FakeService([list(sheets_gateway.REQUIRED_HEADERS)])

    with pytest.raises(sheets_gateway.StatusValidationError):
        sheets_gateway.upsert_rows([_row(RugNo="R-1", Status="Pending")], service=service)

