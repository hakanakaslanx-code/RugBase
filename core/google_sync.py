"""High-level Google Sheets synchronisation utilities."""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from settings import GoogleSyncSettings

logger = logging.getLogger(__name__)


class ColumnType(Enum):
    TEXT = "TEXT"
    NUMBER = "NUMBER"
    INTEGER = "INTEGER"
    DATETIME = "DATETIME"


DEFAULT_TYPE_RULES: Mapping[str, ColumnType] = {
    "SKU": ColumnType.TEXT,
    "RugNo": ColumnType.TEXT,
    "Title": ColumnType.TEXT,
    "Collection": ColumnType.TEXT,
    "Size": ColumnType.TEXT,
    "Price": ColumnType.NUMBER,
    "MSRP": ColumnType.NUMBER,
    "Cost": ColumnType.NUMBER,
    "Qty": ColumnType.INTEGER,
    "Location": ColumnType.TEXT,
    "Condition": ColumnType.TEXT,
    "UpdatedAt": ColumnType.DATETIME,
    "UpdatedBy": ColumnType.TEXT,
    "Notes": ColumnType.TEXT,
}


def column_letter(index: int) -> str:
    """Return the spreadsheet column letter for a 1-indexed column index."""

    if index < 1:
        raise ValueError("Column index must be >= 1")
    result = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


@dataclass
class SheetColumn:
    field: str
    header: str
    type: ColumnType = ColumnType.TEXT


@dataclass
class SheetMapping:
    columns: List[SheetColumn]

    @classmethod
    def from_dict(
        cls, mapping: Mapping[str, str], type_overrides: Optional[Mapping[str, ColumnType]] = None
    ) -> "SheetMapping":
        overrides = dict(DEFAULT_TYPE_RULES)
        if type_overrides:
            overrides.update(type_overrides)
        columns = [
            SheetColumn(field=db_field, header=header, type=overrides.get(header, ColumnType.TEXT))
            for db_field, header in mapping.items()
        ]
        return cls(columns=columns)

    def headers(self) -> List[str]:
        return ["rb_id"] + [column.header for column in self.columns]

    def db_fields(self) -> List[str]:
        return [column.field for column in self.columns]

    def header_for_field(self, field: str) -> Optional[str]:
        for column in self.columns:
            if column.field == field:
                return column.header
        return None

    def column_letter_map(self) -> Mapping[str, str]:
        letters = {}
        for index, header in enumerate(self.headers(), start=1):
            letters[header] = column_letter(index)
        return letters


@dataclass
class RowSnapshot:
    rb_id: str
    values: Mapping[str, Any]
    version: int
    last_pushed_version: int
    updated_at: Optional[str] = None
    updated_by: Optional[str] = None

    @property
    def is_new(self) -> bool:
        return self.last_pushed_version == 0

    @property
    def is_dirty(self) -> bool:
        return self.version > self.last_pushed_version


class BackoffController:
    def __init__(self, base: float = 0.5, maximum: float = 32.0, attempts: int = 5) -> None:
        self.base = base
        self.maximum = maximum
        self.attempts = attempts

    def schedule(self) -> List[float]:
        delays: List[float] = []
        for attempt in range(self.attempts):
            delay = min(self.base * (2**attempt), self.maximum)
            delays.append(delay)
        return delays

    def retry(self, operation: Callable[[], Any]) -> Any:
        last_error: Optional[Exception] = None
        for attempt, delay in enumerate(self.schedule(), start=1):
            try:
                return operation()
            except Exception as exc:  # pragma: no cover - used by integration code
                last_error = exc
                logger.warning("Sync retry %s/%s due to %s", attempt, self.attempts, exc)
                if attempt == self.attempts:
                    break
                time.sleep(delay)
        if last_error:
            raise last_error
        return None


def _format_number(value: Any, *, integer: bool = False) -> str:
    if value in (None, ""):
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if integer:
        return str(int(round(number)))
    return f"{number:.2f}"


def _format_datetime(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, datetime):
        return value.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    return parsed.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def format_cell(value: Any, column_type: ColumnType) -> str:
    if column_type is ColumnType.TEXT:
        return "" if value is None else str(value)
    if column_type is ColumnType.NUMBER:
        return _format_number(value, integer=False)
    if column_type is ColumnType.INTEGER:
        return _format_number(value, integer=True)
    if column_type is ColumnType.DATETIME:
        return _format_datetime(value)
    return "" if value is None else str(value)


def chunk_rows(rows: Sequence[List[Any]], *, max_cells: int, column_count: int) -> List[List[List[Any]]]:
    if column_count <= 0:
        raise ValueError("column_count must be positive")
    if max_cells < column_count:
        max_cells = column_count
    max_rows = max(1, max_cells // column_count)
    chunks: List[List[List[Any]]] = []
    for index in range(0, len(rows), max_rows):
        chunks.append([list(row) for row in rows[index : index + max_rows]])
    return chunks


@dataclass
class Conflict:
    rb_id: str
    local_version: int
    remote_version: int
    field_diffs: Dict[str, Tuple[str, str]] = field(default_factory=dict)


class SheetSynchroniser:
    def __init__(
        self,
        mapping: SheetMapping,
        *,
        type_overrides: Optional[Mapping[str, ColumnType]] = None,
    ) -> None:
        self.mapping = mapping
        self.type_overrides = type_overrides or {}

    def headers(self) -> List[str]:
        return self.mapping.headers()

    def build_row_payload(self, row: RowSnapshot) -> List[str]:
        payload = [row.rb_id]
        for column in self.mapping.columns:
            value = row.values.get(column.field)
            column_type = self.type_overrides.get(column.header, column.type)
            payload.append(format_cell(value, column_type))
        return payload

    def build_value_matrix(self, rows: Iterable[RowSnapshot]) -> List[List[str]]:
        return [self.build_row_payload(row) for row in rows]

    def detect_dirty_rows(self, rows: Iterable[RowSnapshot]) -> List[RowSnapshot]:
        return [row for row in rows if row.is_dirty]

    def split_batches(self, rows: Iterable[RowSnapshot], max_cells: int = 10_000) -> List[List[List[str]]]:
        matrix = self.build_value_matrix(rows)
        return chunk_rows(matrix, max_cells=max_cells, column_count=len(self.headers()))

    def detect_conflicts(
        self,
        local_rows: Iterable[RowSnapshot],
        remote_versions: Mapping[str, int],
        remote_values: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> List[Conflict]:
        conflicts: List[Conflict] = []
        for row in local_rows:
            sheet_version = remote_versions.get(row.rb_id)
            if sheet_version is None:
                continue
            if sheet_version > row.last_pushed_version:
                field_diffs: Dict[str, Tuple[str, str]] = {}
                if remote_values:
                    remote_row = remote_values.get(row.rb_id, {})
                    for column in self.mapping.columns:
                        header = column.header
                        remote_value = remote_row.get(header, "")
                        local_value = format_cell(row.values.get(column.field), column.type)
                        if str(remote_value) != str(local_value):
                            field_diffs[header] = (str(local_value), str(remote_value))
                conflicts.append(
                    Conflict(
                        rb_id=row.rb_id,
                        local_version=row.version,
                        remote_version=sheet_version,
                        field_diffs=field_diffs,
                    )
                )
        return conflicts


def build_config_sheet(mapping: SheetMapping) -> List[List[str]]:
    headers = ["Column", "Letter"]
    rows = [[header, letter] for header, letter in mapping.column_letter_map().items()]
    return [headers, *rows]


def serialise_sync_payload(rows: Iterable[RowSnapshot], mapping: SheetMapping) -> str:
    payload = {
        "headers": mapping.headers(),
        "rows": [
            {
                "rb_id": row.rb_id,
                "values": {field: row.values.get(field) for field in mapping.db_fields()},
                "version": row.version,
                "last_pushed_version": row.last_pushed_version,
                "updated_at": row.updated_at,
                "updated_by": row.updated_by,
            }
            for row in rows
        ],
    }
    return json.dumps(payload, ensure_ascii=False)


def default_mapping(settings: GoogleSyncSettings, db_fields: Sequence[str]) -> SheetMapping:
    mapping_dict = settings.mapping_dict()
    if not mapping_dict:
        mapping_dict = {field: field for field in db_fields}
    return SheetMapping.from_dict(mapping_dict)


__all__ = [
    "BackoffController",
    "ColumnType",
    "Conflict",
    "RowSnapshot",
    "SheetColumn",
    "SheetMapping",
    "SheetSynchroniser",
    "build_config_sheet",
    "chunk_rows",
    "column_letter",
    "default_mapping",
    "format_cell",
    "serialise_sync_payload",
]
