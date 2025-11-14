"""Lightweight workbook service backed by JSON files simulating Excel sheets."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple


@dataclass(frozen=True)
class _CellRef:
    row: Optional[int]
    column: Optional[int]


class _ExcelRequest:
    def __init__(self, callback: Callable[[], Mapping[str, object]]) -> None:
        self._callback = callback

    def execute(self) -> Mapping[str, object]:
        return self._callback()


class ExcelValuesApi:
    def __init__(self, workbook_path: Path) -> None:
        self._workbook_path = workbook_path

    def get(self, spreadsheetId: str, range: str) -> _ExcelRequest:  # noqa: N803 - API compatibility
        return _ExcelRequest(lambda: self._handle_get(range))

    def batchGet(  # noqa: N802 - API compatibility
        self,
        spreadsheetId: str,
        ranges: Sequence[str],
        *,
        majorDimension: str = "ROWS",
    ) -> _ExcelRequest:
        return _ExcelRequest(lambda: self._handle_batch_get(ranges, majorDimension))

    def batchUpdate(  # noqa: N802 - API compatibility
        self,
        spreadsheetId: str,
        body: Mapping[str, object],
    ) -> _ExcelRequest:
        return _ExcelRequest(lambda: self._handle_batch_update(body))

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------
    def _load(self) -> Dict[str, List[List[str]]]:
        path = self._workbook_path
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        sheets = payload.get("sheets", {})
        return {title: [list(map(str, row)) for row in rows] for title, rows in sheets.items()}

    def _save(self, sheets: Mapping[str, Sequence[Sequence[str]]]) -> None:
        path = self._workbook_path
        if path.parent:
            path.parent.mkdir(parents=True, exist_ok=True)
        payload = {title: [list(row) for row in rows] for title, rows in sheets.items()}
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"sheets": payload}, handle, indent=2)

    # ------------------------------------------------------------------
    # Range operations
    # ------------------------------------------------------------------
    def _handle_get(self, range_spec: str) -> Mapping[str, object]:
        sheets = self._load()
        sheet, start, end = _parse_range(range_spec)
        rows = sheets.get(sheet, [])
        return {"values": _slice_rows(rows, start, end)}

    def _handle_batch_get(
        self, ranges: Sequence[str], major_dimension: str
    ) -> Mapping[str, object]:
        if major_dimension != "ROWS":
            raise ValueError("Only ROWS major dimension is supported")
        sheets = self._load()
        value_ranges: List[Mapping[str, object]] = []
        for range_spec in ranges:
            sheet, start, end = _parse_range(range_spec)
            rows = sheets.get(sheet, [])
            value_ranges.append({"range": range_spec, "values": _slice_rows(rows, start, end)})
        return {"valueRanges": value_ranges}

    def _handle_batch_update(self, body: Mapping[str, object]) -> Mapping[str, object]:
        sheets = self._load()
        data_entries = body.get("data", [])
        pending: Dict[str, MutableMapping[Tuple[int, int], str]] = {}
        bounds: Dict[str, Tuple[int, int]] = {}
        for entry in data_entries:
            if not isinstance(entry, Mapping):
                continue
            range_spec = str(entry.get("range", ""))
            values = entry.get("values", [])
            if not isinstance(values, Sequence):
                continue
            title, start, end = _parse_range(range_spec)
            matrix = pending.setdefault(title, {})
            max_row = end.row or 0
            max_col = end.column or 0
            base_row = start.row or 1
            base_col = start.column or 1
            for row_offset, row in enumerate(values):
                if not isinstance(row, Sequence):
                    continue
                for col_offset, cell in enumerate(row):
                    matrix[(base_row + row_offset, base_col + col_offset)] = "" if cell is None else str(cell)
                max_row = max(max_row, base_row + row_offset)
                if row:
                    max_col = max(max_col, base_col + len(row) - 1)
            bounds[title] = (
                max(max_row, bounds.get(title, (0, 0))[0]),
                max(max_col, bounds.get(title, (0, 0))[1]),
            )

        for title, matrix in pending.items():
            max_row, max_col = bounds.get(title, (0, 0))
            if max_row == 0 or max_col == 0:
                sheets[title] = []
                continue
            rows = [["" for _ in range(max_col)] for _ in range(max_row)]
            for (row_index, col_index), value in matrix.items():
                rows[row_index - 1][col_index - 1] = value
            sheets[title] = rows

        if pending:
            self._save(sheets)
        return {}


class ExcelSpreadsheetsApi:
    def __init__(self, workbook_path: Path) -> None:
        self._workbook_path = workbook_path

    def values(self) -> ExcelValuesApi:  # noqa: D401 - compatibility proxy
        return ExcelValuesApi(self._workbook_path)

    def get(
        self,
        spreadsheetId: str,
        includeGridData: bool = False,
        ranges: Iterable[str] | None = None,
    ) -> _ExcelRequest:
        def _noop() -> Mapping[str, object]:
            path = self._workbook_path
            if not path.exists() and path.parent:
                path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists():
                with open(path, "w", encoding="utf-8") as handle:
                    json.dump({"sheets": {}}, handle)
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            sheets_payload = [
                {"properties": {"title": str(title)}}
                for title in payload.get("sheets", {}).keys()
            ]
            return {"spreadsheetId": str(path), "sheets": sheets_payload}

        return _ExcelRequest(_noop)

    def batchUpdate(
        self,
        spreadsheetId: str,
        body: Mapping[str, object],
    ) -> _ExcelRequest:
        return _ExcelRequest(lambda: self._handle_batch_update(body))

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------
    def _load(self) -> Dict[str, List[List[str]]]:
        path = self._workbook_path
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        sheets = payload.get("sheets", {})
        return {title: [list(map(str, row)) for row in rows] for title, rows in sheets.items()}

    def _save(self, sheets: Mapping[str, Sequence[Sequence[str]]]) -> None:
        path = self._workbook_path
        if path.parent:
            path.parent.mkdir(parents=True, exist_ok=True)
        payload = {title: [list(row) for row in rows] for title, rows in sheets.items()}
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"sheets": payload}, handle, indent=2)

    # ------------------------------------------------------------------
    # Batch update helpers
    # ------------------------------------------------------------------
    def _handle_batch_update(self, body: Mapping[str, object]) -> Mapping[str, object]:
        sheets = self._load()
        requests = body.get("requests", [])
        for request in requests:
            if not isinstance(request, Mapping):
                continue
            add_sheet = request.get("addSheet")
            if not isinstance(add_sheet, Mapping):
                continue
            properties = add_sheet.get("properties", {})
            if not isinstance(properties, Mapping):
                continue
            title = properties.get("title")
            if not isinstance(title, str):
                continue
            sheets.setdefault(title, [])

        self._save(sheets)
        return {}


class ExcelService:
    """Minimal Sheets API drop-in that stores worksheets in JSON files."""

    def __init__(self, workbook_path: Path) -> None:
        self._workbook_path = workbook_path

    def spreadsheets(self) -> ExcelSpreadsheetsApi:  # noqa: D401 - compatibility proxy
        return ExcelSpreadsheetsApi(self._workbook_path)


_A1_RE = re.compile(r"^'?(?P<title>[^']+?)'?!?(?P<cells>[A-Za-z0-9:]+)?$")
_CELL_RE = re.compile(r"^(?P<col>[A-Z]+)(?P<row>\d+)?$")


def _slice_rows(rows: Sequence[Sequence[str]], start: _CellRef, end: _CellRef) -> List[List[str]]:
    if not rows:
        return []
    min_row = max(1, start.row or 1)
    min_col = max(1, start.column or 1)
    max_row = end.row or len(rows)
    max_col = end.column or max(len(row) for row in rows)
    sliced: List[List[str]] = []
    for row_index in range(min_row - 1, min(max_row, len(rows))):
        row = rows[row_index]
        current: List[str] = []
        for col_index in range(min_col - 1, min(max_col, len(row))):
            current.append(str(row[col_index]))
        while current and current[-1] == "":
            current.pop()
        sliced.append(current)
    while sliced and all(cell == "" for cell in sliced[-1]):
        sliced.pop()
    return sliced


def _parse_range(range_spec: str) -> Tuple[str, _CellRef, _CellRef]:
    match = _A1_RE.match(range_spec.strip())
    if not match:
        raise ValueError(f"Invalid range specification: {range_spec!r}")
    title = match.group("title").replace("''", "'")
    cells = match.group("cells") or "A1:A1"
    if ":" in cells:
        start_text, end_text = cells.split(":", 1)
    else:
        start_text = end_text = cells
    start = _parse_cell(start_text)
    end = _parse_cell(end_text)
    return title, start, end


def _parse_cell(value: str) -> _CellRef:
    value = value.strip().upper()
    if not value:
        return _CellRef(row=None, column=None)
    match = _CELL_RE.match(value)
    if not match:
        raise ValueError(f"Invalid cell reference: {value!r}")
    column_label = match.group("col") or "A"
    row_text = match.group("row")
    column = _column_index(column_label)
    row = int(row_text) if row_text else None
    return _CellRef(row=row, column=column)


def _column_index(label: str) -> int:
    index = 0
    for char in label:
        if not char.isalpha():
            break
        index = index * 26 + (ord(char) - ord("A") + 1)
    return max(1, index)


__all__ = ["ExcelService"]

