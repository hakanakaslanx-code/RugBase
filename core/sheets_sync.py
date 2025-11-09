"""Google Sheets synchronisation helpers for RugBase.

This module implements a high-performance delta synchronisation pipeline
between the local RugBase SQLite database and the Google Sheets inventory
spreadsheet.  The implementation follows the requirements defined in the
project specification and provides the following public entry points:

``push``
    Push locally changed rows to Google Sheets in 300-500 sized batches,
    performing automatic retry with exponential backoff.

``pull``
    Fetch remote updates newer than the last local pull timestamp and
    upsert them into SQLite using a last-write-wins strategy.

``resolve_conflict``
    Helper implementing last-write-wins conflict resolution with backup
    generation.

``calc_hash``
    Produce a stable SHA-256 hash for a Sheet row by serialising the row
    JSON with deterministic ordering.

In addition to the public API this module exposes a number of utilities
that are used by the test-suite such as ``chunked`` and
``detect_local_deltas``.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

import db
from core import app_paths
from core.version import __version__ as APP_VERSION

logger = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:  # pragma: no cover - runtime guard
    service_account = None  # type: ignore[assignment]
    build = None  # type: ignore[assignment]

    class HttpError(Exception):
        """Fallback error type when googleapiclient is unavailable."""

    GOOGLE_API_AVAILABLE = False
else:  # pragma: no cover - simple attribute assignment
    GOOGLE_API_AVAILABLE = True


# ---------------------------------------------------------------------------
# Sheet configuration
# ---------------------------------------------------------------------------
HEADERS: List[str] = [
    "RowID",
    "RugNo",
    "SKU",
    "Title",
    "Collection",
    "Style",
    "Origin",
    "Material",
    "Weave",
    "Size",
    "Size_Std",
    "Color",
    "PileHeight",
    "Age",
    "Condition",
    "MSRP",
    "Price",
    "Cost",
    "Currency",
    "Location",
    "Qty",
    "Status",
    "Consignment",
    "RoomScene",
    "ImageURLs",
    "Barcode",
    "Tags",
    "Notes",
    "UpdatedAt",
    "Hash",
    "Deleted",
]

DEFAULT_WORKSHEET_TITLE = "items"
META_SHEET_TITLE = "meta"
LOG_SHEET_TITLE = "sync_logs"
SCOPES: Iterable[str] = ("https://www.googleapis.com/auth/spreadsheets",)

STATUS_ALLOWED_VALUES: Tuple[str, ...] = ("active", "archived", "sold", "reserved")
MAX_BATCH_ROWS = 500
MIN_BATCH_ROWS = 300
MAX_RETRY_ATTEMPTS = 5
BACKOFF_SCHEDULE = (1, 2, 4, 8, 16)

LOCAL_STATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sheet_sync_state (
    row_id TEXT PRIMARY KEY,
    hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""

LOCAL_META_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sheet_sync_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"""

META_KEYS = ("db_version", "last_sync_utc", "last_pull_utc")

LOCAL_TO_SHEET_FIELD_MAP: Mapping[str, Optional[str]] = {
    "RowID": "item_id",
    "RugNo": "rug_no",
    "SKU": "upc",
    "Title": "design",
    "Collection": "collection",
    "Style": "style",
    "Origin": "origin",
    "Material": None,
    "Weave": None,
    "Size": "st_size",
    "Size_Std": "a_size",
    "Color": "ground",
    "PileHeight": None,
    "Age": None,
    "Condition": None,
    "MSRP": "msrp",
    "Price": "retail",
    "Cost": "cost",
    "Currency": None,
    "Location": "location",
    "Qty": "qty",
    "Status": "status",
    "Consignment": "consignment_id",
    "RoomScene": None,
    "ImageURLs": "image_file_name",
    "Barcode": "upc",
    "Tags": "brand_name",
    "Notes": None,
    "UpdatedAt": "updated_at",
    "Deleted": None,
}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------
class SheetsSyncError(Exception):
    """Base exception for Google Sheets synchronisation errors."""


class MissingDependencyError(SheetsSyncError):
    """Raised when required Google client libraries are not available."""


class CredentialsFileNotFoundError(SheetsSyncError):
    """Raised when the configured credentials file cannot be located."""


class CredentialsFileInvalidError(SheetsSyncError):
    """Raised when the configured credentials file is invalid."""


class SpreadsheetAccessError(SheetsSyncError):
    """Raised when the Google Sheets API returns an error."""


# ---------------------------------------------------------------------------
# Dataclasses and helper types
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class SheetRow:
    """Representation of a row prepared for Google Sheets synchronisation."""

    row_id: str
    values: Dict[str, str]
    hash: str
    row_index: Optional[int] = None

    def as_list(self) -> List[str]:
        return [self.values.get(header, "") for header in HEADERS]


# ---------------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------------
def is_api_available() -> bool:
    """Return ``True`` when the Google Sheets client libraries are present."""

    return GOOGLE_API_AVAILABLE


def parse_spreadsheet_id(value: str) -> str:
    """Normalise a spreadsheet identifier from raw input or URL."""

    if not value:
        return ""
    value = value.strip()
    if "/spreadsheets/d/" in value:
        value = value.split("/spreadsheets/d/", 1)[1]
        value = value.split("/", 1)[0]
    if "?" in value:
        value = value.split("?", 1)[0]
    if "#" in value:
        value = value.split("#", 1)[0]
    return value


def _require_api() -> None:
    if not GOOGLE_API_AVAILABLE:
        raise MissingDependencyError(
            "google-api-python-client bulunamadı. Lütfen bağımlılıkları yükleyin."
        )


def get_client(credentials_path: str):
    """Return an authenticated Sheets API client using the service account."""

    _require_api()
    path = Path(os.path.expanduser(credentials_path)).resolve()
    if not path.exists():
        raise CredentialsFileNotFoundError(f"Kimlik dosyası bulunamadı: {path}")

    try:
        credentials = service_account.Credentials.from_service_account_file(  # type: ignore[union-attr]
            str(path), scopes=SCOPES
        )
    except ValueError as exc:  # pragma: no cover - invalid key file
        raise CredentialsFileInvalidError(
            "Kimlik dosyası okunamadı. JSON formatını doğrulayın."
        ) from exc
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)  # type: ignore[call-arg]


def calc_hash(row: Mapping[str, Any]) -> str:
    """Return a deterministic SHA-256 hash for the given Sheet row."""

    normalised: Dict[str, str] = {}
    for key in HEADERS:
        if key == "Hash":
            continue
        value = row.get(key, "") if isinstance(row, Mapping) else ""
        if value is None:
            normalised[key] = ""
        else:
            normalised[key] = str(value)
    payload = json.dumps(normalised, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def chunked(sequence: Sequence[Any], max_size: int = MAX_BATCH_ROWS) -> Iterator[Sequence[Any]]:
    """Yield slices of ``sequence`` containing at most ``max_size`` entries."""

    if max_size <= 0:
        raise ValueError("max_size must be positive")
    total = len(sequence)
    for start in range(0, total, max_size):
        yield sequence[start : start + max_size]


def detect_local_deltas(
    rows: Sequence[SheetRow],
    previous_hashes: Mapping[str, str],
) -> Tuple[List[SheetRow], List[SheetRow]]:
    """Return ``(new_rows, changed_rows)`` using stored hash state."""

    new_rows: List[SheetRow] = []
    changed_rows: List[SheetRow] = []
    for row in rows:
        known_hash = previous_hashes.get(row.row_id)
        if known_hash is None:
            new_rows.append(row)
        elif known_hash != row.hash:
            changed_rows.append(row)
    return new_rows, changed_rows


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------
def _connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or db.DB_PATH
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    with connection:
        connection.execute(LOCAL_STATE_TABLE_SQL)
        connection.execute(LOCAL_META_TABLE_SQL)
    return connection


def _read_local_meta(conn: sqlite3.Connection) -> Dict[str, str]:
    cursor = conn.execute("SELECT key, value FROM sheet_sync_meta")
    return {row["key"]: row["value"] for row in cursor.fetchall()}


def _write_local_meta(conn: sqlite3.Connection, updates: Mapping[str, str]) -> None:
    timestamp = datetime.utcnow().replace(microsecond=0).isoformat()
    with conn:
        conn.executemany(
            "INSERT INTO sheet_sync_meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [(key, value) for key, value in updates.items()],
        )
        conn.execute(
            "INSERT INTO sheet_sync_meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("last_state_update", timestamp),
        )


def _load_previous_hashes(conn: sqlite3.Connection) -> Dict[str, str]:
    cursor = conn.execute("SELECT row_id, hash FROM sheet_sync_state")
    return {row["row_id"]: row["hash"] for row in cursor.fetchall()}


def _update_local_hash_state(conn: sqlite3.Connection, rows: Sequence[SheetRow]) -> None:
    timestamp = datetime.utcnow().replace(microsecond=0).isoformat()
    payload = [
        (row.row_id, row.hash, timestamp)
        for row in rows
    ]
    with conn:
        conn.executemany(
            "INSERT INTO sheet_sync_state(row_id, hash, updated_at) VALUES(?, ?, ?) "
            "ON CONFLICT(row_id) DO UPDATE SET hash=excluded.hash, updated_at=excluded.updated_at",
            payload,
        )


def _sqlite_row_to_sheet(row: sqlite3.Row) -> SheetRow:
    record = dict(row)
    values: Dict[str, str] = {header: "" for header in HEADERS}
    for header, source in LOCAL_TO_SHEET_FIELD_MAP.items():
        if source:
            raw_value = record.get(source)
            values[header] = "" if raw_value is None else str(raw_value)
        else:
            values[header] = ""
    status = (record.get("status") or "").strip()
    values["Deleted"] = "TRUE" if status in {"archived", "deleted"} else ""
    values["Currency"] = values.get("Currency") or "USD"
    values["UpdatedAt"] = values.get("UpdatedAt") or record.get("updated_at") or ""
    if not values["UpdatedAt"]:
        values["UpdatedAt"] = datetime.utcnow().replace(microsecond=0).isoformat()
    qty_value = record.get("qty")
    values["Qty"] = str(qty_value if qty_value is not None else 0)
    consignment = record.get("consignment_id")
    values["Consignment"] = "" if consignment is None else str(consignment)
    values["Tags"] = values.get("Tags") or (record.get("brand_name") or "")
    values["Barcode"] = values.get("Barcode") or (record.get("upc") or "")
    values["Title"] = values.get("Title") or (record.get("design") or "")
    values["RowID"] = str(record.get("item_id"))
    values["Hash"] = calc_hash(values)
    return SheetRow(row_id=values["RowID"], values=values, hash=values["Hash"])


def _fetch_local_rows(conn: sqlite3.Connection) -> List[SheetRow]:
    cursor = conn.execute(
        "SELECT item_id, rug_no, upc, roll_no, v_rug_no, v_collection, collection, "
        "v_design, design, brand_name, ground, border, a_size, st_size, area, type, "
        "rate, amount, shape, style, image_file_name, origin, retail, sp, msrp, cost, "
        "qty, created_at, updated_at, version, status, location, consignment_id "
        "FROM item"
    )
    return [_sqlite_row_to_sheet(row) for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Google Sheets helpers
# ---------------------------------------------------------------------------
def _call_with_retry(func: Callable[[], Any], description: str) -> Tuple[Any, int]:
    """Execute ``func`` applying exponential backoff for retriable errors."""

    attempt = 0
    while True:
        try:
            result = func()
        except HttpError as exc:  # pragma: no cover - network interaction
            status = getattr(exc, "status_code", None) or getattr(getattr(exc, "resp", None), "status", None)
            if status not in {429, 500, 502, 503, 504}:
                raise
            if attempt >= MAX_RETRY_ATTEMPTS - 1:
                raise
            delay = BACKOFF_SCHEDULE[min(attempt, len(BACKOFF_SCHEDULE) - 1)]
            attempt += 1
            logger.warning(
                "Sheets API %s hatası (%s). %ss sonra tekrar denenecek (%d/%d)",
                description,
                status,
                delay,
                attempt,
                MAX_RETRY_ATTEMPTS,
            )
            time.sleep(delay)
            continue
        else:
            return result, attempt


def _values_batch_get(service, spreadsheet_id: str, ranges: Sequence[str]) -> Dict[str, Any]:
    request = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheet_id,
        ranges=list(ranges),
        majorDimension="ROWS",
    )
    result, _ = _call_with_retry(request.execute, "values.batchGet")
    return result


def _values_batch_update(service, spreadsheet_id: str, data: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], int]:
    request = service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"valueInputOption": "RAW", "data": data},
    )
    return _call_with_retry(request.execute, "values.batchUpdate")


def _spreadsheet_get(service, spreadsheet_id: str) -> Dict[str, Any]:
    request = service.spreadsheets().get(spreadsheetId=spreadsheet_id, includeGridData=False)
    result, _ = _call_with_retry(request.execute, "spreadsheets.get")
    return result


def _column_to_index(header: str) -> int:
    try:
        return HEADERS.index(header)
    except ValueError:
        raise SheetsSyncError(f"Unknown header: {header}")


def _column_a1(column_index: int) -> str:
    column_index += 1
    label = ""
    while column_index:
        column_index, remainder = divmod(column_index - 1, 26)
        label = chr(65 + remainder) + label
    return label


def _sheet_range(row_index: int) -> str:
    start = _column_a1(0)
    end = _column_a1(len(HEADERS) - 1)
    return f"{start}{row_index}:{end}{row_index}"


def _ensure_sheet_structure(
    service,
    spreadsheet_id: str,
    worksheet_title: str,
) -> int:
    """Ensure worksheet, meta, and log sheets exist and return worksheet ID."""

    metadata = _spreadsheet_get(service, spreadsheet_id)
    sheets = metadata.get("sheets", []) if isinstance(metadata, dict) else []
    worksheet_id: Optional[int] = None
    meta_id: Optional[int] = None
    log_id: Optional[int] = None

    for sheet in sheets:
        properties = sheet.get("properties", {})
        if properties.get("title") == worksheet_title:
            worksheet_id = properties.get("sheetId")
        if properties.get("title") == META_SHEET_TITLE:
            meta_id = properties.get("sheetId")
        if properties.get("title") == LOG_SHEET_TITLE:
            log_id = properties.get("sheetId")

    requests: List[Dict[str, Any]] = []

    if worksheet_id is None:
        requests.append(
            {
                "addSheet": {
                    "properties": {
                        "title": worksheet_title,
                        "gridProperties": {"rowCount": 2, "columnCount": len(HEADERS)},
                    }
                }
            }
        )
    if meta_id is None:
        requests.append(
            {
                "addSheet": {
                    "properties": {
                        "title": META_SHEET_TITLE,
                        "hidden": True,
                        "gridProperties": {"rowCount": 10, "columnCount": 2},
                    }
                }
            }
        )
    if log_id is None:
        requests.append(
            {
                "addSheet": {
                    "properties": {
                        "title": LOG_SHEET_TITLE,
                        "gridProperties": {"rowCount": 100, "columnCount": 6},
                    }
                }
            }
        )

    if requests:
        request = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        )
        _call_with_retry(request.execute, "spreadsheets.batchUpdate")
        metadata = _spreadsheet_get(service, spreadsheet_id)
        sheets = metadata.get("sheets", [])
        for sheet in sheets:
            properties = sheet.get("properties", {})
            if properties.get("title") == worksheet_title:
                worksheet_id = properties.get("sheetId")
            if properties.get("title") == META_SHEET_TITLE:
                meta_id = properties.get("sheetId")
            if properties.get("title") == LOG_SHEET_TITLE:
                log_id = properties.get("sheetId")

    if worksheet_id is None:
        raise SpreadsheetAccessError("Worksheet bulunamadı veya oluşturulamadı.")

    # Ensure headers present
    header_range = f"{worksheet_title}!A1:{_column_a1(len(HEADERS) - 1)}1"
    current_headers = _values_batch_get(service, spreadsheet_id, [header_range])
    values = current_headers.get("valueRanges", [{}])[0].get("values", [])
    if not values or values[0] != HEADERS:
        _values_batch_update(
            service,
            spreadsheet_id,
            [
                {
                    "range": header_range,
                    "values": [HEADERS],
                }
            ],
        )

    # Apply formatting (freeze header, filters, validation, currency)
    status_index = _column_to_index("Status")
    price_columns = [
        _column_to_index("Price"),
        _column_to_index("Cost"),
        _column_to_index("MSRP"),
    ]
    format_requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": worksheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": worksheet_id,
                        "startRowIndex": 0,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(HEADERS),
                    }
                }
            }
        },
        {
            "setDataValidation": {
                "range": {
                    "sheetId": worksheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": status_index,
                    "endColumnIndex": status_index + 1,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [
                            {"userEnteredValue": value} for value in STATUS_ALLOWED_VALUES
                        ],
                    },
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        },
    ]
    for column_index in price_columns:
        format_requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": column_index,
                        "endColumnIndex": column_index + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"}
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

    request = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": format_requests},
    )
    _call_with_retry(request.execute, "spreadsheets.batchUpdate")

    # Ensure meta sheet headers exist
    meta_header_range = f"{META_SHEET_TITLE}!A1:B1"
    meta_current = _values_batch_get(service, spreadsheet_id, [meta_header_range])
    meta_values = meta_current.get("valueRanges", [{}])[0].get("values", [])
    if not meta_values:
        _values_batch_update(
            service,
            spreadsheet_id,
            [
                {
                    "range": meta_header_range,
                    "values": [["key", "value"]],
                }
            ],
        )

    # Ensure log sheet headers exist
    log_header_range = f"{LOG_SHEET_TITLE}!A1:F1"
    log_current = _values_batch_get(service, spreadsheet_id, [log_header_range])
    log_values = log_current.get("valueRanges", [{}])[0].get("values", [])
    if not log_values:
        _values_batch_update(
            service,
            spreadsheet_id,
            [
                {
                    "range": log_header_range,
                    "values": [["timestamp", "direction", "action", "rows", "duration", "retries"]],
                }
            ],
        )

    return worksheet_id


def _append_sync_log(
    service,
    spreadsheet_id: str,
    *,
    direction: str,
    action: str,
    rows: int,
    duration: float,
    retries: int,
) -> None:
    timestamp = datetime.utcnow().replace(microsecond=0).isoformat()
    log_sheet_range = f"{LOG_SHEET_TITLE}!A:A"
    existing = _values_batch_get(service, spreadsheet_id, [log_sheet_range])
    entries = existing.get("valueRanges", [{}])[0].get("values", [])
    next_row = len(entries) + 1
    data = [
        {
            "range": f"{LOG_SHEET_TITLE}!A{next_row}:F{next_row}",
            "values": [[timestamp, direction, action, str(rows), f"{duration:.3f}", str(retries)]],
        }
    ]
    _values_batch_update(service, spreadsheet_id, data)


def _read_remote_rows(
    service,
    spreadsheet_id: str,
    worksheet_title: str,
) -> List[SheetRow]:
    range_a1 = f"{worksheet_title}!A1:{_column_a1(len(HEADERS) - 1)}"
    payload = _values_batch_get(service, spreadsheet_id, [range_a1])
    value_ranges = payload.get("valueRanges", [])
    rows: List[SheetRow] = []
    if not value_ranges:
        return rows
    values = value_ranges[0].get("values", [])
    if not values:
        return rows
    for index, raw_row in enumerate(values[1:], start=2):  # Skip header row
        row_values: Dict[str, str] = {header: "" for header in HEADERS}
        for idx, header in enumerate(HEADERS):
            if idx < len(raw_row):
                row_values[header] = raw_row[idx]
        row_id = row_values.get("RowID", "").strip()
        if not row_id:
            continue
        row_values["Hash"] = row_values.get("Hash") or calc_hash(row_values)
        rows.append(
            SheetRow(row_id=row_id, values=row_values, hash=row_values["Hash"], row_index=index)
        )
    return rows


def _write_remote_meta(
    service,
    spreadsheet_id: str,
    updates: Mapping[str, str],
) -> None:
    if not updates:
        return
    existing = _values_batch_get(service, spreadsheet_id, [f"{META_SHEET_TITLE}!A:B"])
    rows = existing.get("valueRanges", [{}])[0].get("values", [])
    meta_map: Dict[str, str] = {}
    for row in rows[1:]:
        if len(row) >= 2:
            meta_map[row[0]] = row[1]
    meta_map.update(updates)
    ordered = [["key", "value"]]
    for key in META_KEYS:
        if key in meta_map:
            ordered.append([key, meta_map[key]])
    for key, value in meta_map.items():
        if key not in META_KEYS:
            ordered.append([key, value])
    data = [
        {
            "range": f"{META_SHEET_TITLE}!A1:B{len(ordered)}",
            "values": ordered,
        }
    ]
    _values_batch_update(service, spreadsheet_id, data)


# ---------------------------------------------------------------------------
# Conflict resolution and conversion helpers
# ---------------------------------------------------------------------------
def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        if value.endswith("Z"):
            try:
                return datetime.fromisoformat(value[:-1] + "+00:00")
            except ValueError:
                return None
    return None


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value)))
    except (TypeError, ValueError):
        return default


def resolve_conflict(
    local_row: Mapping[str, Any],
    remote_row: Mapping[str, Any],
    *,
    backup_prefix: str = "sheet-conflict",
) -> Dict[str, Any]:
    """Return the winning row using last-write-wins and back up the other."""

    local_ts = _parse_timestamp(local_row.get("UpdatedAt"))
    remote_ts = _parse_timestamp(remote_row.get("UpdatedAt"))
    backup_dir = app_paths.ensure_directory(app_paths.BACKUP_DIR)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    row_id = remote_row.get("RowID") or local_row.get("RowID") or "unknown"

    def _backup(row: Mapping[str, Any], suffix: str) -> None:
        path = backup_dir / f"{backup_prefix}-{row_id}-{suffix}-{timestamp}.bak.json"
        try:
            with path.open("w", encoding="utf-8") as handle:
                json.dump(row, handle, ensure_ascii=False, indent=2)
        except OSError:  # pragma: no cover - filesystem guard
            logger.warning("Çakışma yedeği yazılamadı: %s", path, exc_info=True)

    if remote_ts and (not local_ts or remote_ts >= local_ts):
        _backup(local_row, "local")
        return dict(remote_row)
    if local_ts and (not remote_ts or local_ts > remote_ts):
        _backup(remote_row, "remote")
        return dict(local_row)
    # Tie breaker: prefer remote row to ensure convergence
    _backup(local_row, "local")
    return dict(remote_row)


def _sheet_row_to_db_payload(row: Mapping[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "item_id": row.get("RowID"),
        "rug_no": row.get("RugNo"),
        "upc": row.get("SKU") or row.get("Barcode"),
        "design": row.get("Title"),
        "collection": row.get("Collection"),
        "style": row.get("Style"),
        "origin": row.get("Origin"),
        "st_size": row.get("Size"),
        "a_size": row.get("Size_Std"),
        "ground": row.get("Color"),
        "msrp": row.get("MSRP"),
        "retail": row.get("Price"),
        "cost": row.get("Cost"),
        "location": row.get("Location"),
        "qty": _to_int(row.get("Qty"), default=0),
        "status": row.get("Status") or "active",
        "consignment_id": row.get("Consignment"),
        "image_file_name": row.get("ImageURLs"),
        "brand_name": row.get("Tags"),
        "updated_at": row.get("UpdatedAt"),
    }
    if str(row.get("Deleted", "")).upper() == "TRUE":
        payload["status"] = "archived"
        payload["qty"] = 0
    return payload


# ---------------------------------------------------------------------------
# Public sync operations
# ---------------------------------------------------------------------------
def push(
    spreadsheet_id: str,
    credential_path: str,
    *,
    worksheet_title: str = DEFAULT_WORKSHEET_TITLE,
    db_path: Optional[str] = None,
    log_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, int]:
    """Push local changes to Google Sheets and return sync statistics."""

    parsed_id = parse_spreadsheet_id(spreadsheet_id)
    if not parsed_id:
        raise SpreadsheetAccessError("Geçerli bir Sheet ID gerekli.")

    service = get_client(credential_path)
    worksheet_id = _ensure_sheet_structure(service, parsed_id, worksheet_title)

    start = time.monotonic()
    with _connect(db_path) as conn:
        previous_hashes = _load_previous_hashes(conn)
        local_rows = _fetch_local_rows(conn)

    new_rows, changed_rows = detect_local_deltas(local_rows, previous_hashes)

    remote_rows = _read_remote_rows(service, parsed_id, worksheet_title)
    remote_index: Dict[str, SheetRow] = {row.row_id: row for row in remote_rows}
    updates: List[Tuple[int, SheetRow]] = []
    for row in changed_rows:
        remote = remote_index.get(row.row_id)
        row.row_index = remote.row_index if remote else row.row_index
        if row.row_index is None and remote:
            row.row_index = remote.row_index
        if row.row_index is None:
            new_rows.append(row)
        else:
            updates.append((row.row_index, row))

    # Determine append start row
    next_row_index = max((row.row_index or 1 for row in remote_rows), default=1) + 1

    total_written = 0
    total_retries = 0

    if updates:
        batches: List[List[Tuple[int, SheetRow]]] = [list(chunk) for chunk in chunked(updates, MAX_BATCH_ROWS)]
        for batch in batches:
            data = []
            for row_index, row in batch:
                a1_range = f"{worksheet_title}!{_sheet_range(row_index)}"
                data.append({"range": a1_range, "values": [row.as_list()]})
            _, retries = _values_batch_update(service, parsed_id, data)
            total_written += len(batch)
            total_retries += retries
            if log_callback:
                log_callback(
                    f"{len(batch)} satır güncellendi (retry={retries})."
                )

    if new_rows:
        batches = [list(chunk) for chunk in chunked(new_rows, MAX_BATCH_ROWS)]
        for batch in batches:
            data = []
            for row in batch:
                row.row_index = next_row_index
                data.append(
                    {
                        "range": f"{worksheet_title}!{_sheet_range(next_row_index)}",
                        "values": [row.as_list()],
                    }
                )
                next_row_index += 1
            _, retries = _values_batch_update(service, parsed_id, data)
            total_written += len(batch)
            total_retries += retries
            if log_callback:
                log_callback(
                    f"{len(batch)} yeni satır eklendi (retry={retries})."
                )

    if total_written:
        now_iso = datetime.utcnow().replace(microsecond=0).isoformat()
        with _connect(db_path) as conn:
            _update_local_hash_state(conn, local_rows)
            _write_local_meta(conn, {"last_sync_utc": now_iso, "db_version": APP_VERSION})
        _write_remote_meta(
            service,
            parsed_id,
            {"last_sync_utc": now_iso, "db_version": APP_VERSION},
        )

    duration = time.monotonic() - start
    _append_sync_log(
        service,
        parsed_id,
        direction="push",
        action="full" if total_written == len(local_rows) else "delta",
        rows=total_written,
        duration=duration,
        retries=total_retries,
    )

    new_count = len([row for row in new_rows if row.row_index and row.row_index >= 0])
    changed_count = len(updates)
    return {
        "updated": changed_count,
        "new": new_count,
        "changed": changed_count,
        "total": total_written,
    }


def pull(
    spreadsheet_id: str,
    credential_path: str,
    *,
    worksheet_title: str = DEFAULT_WORKSHEET_TITLE,
    db_path: Optional[str] = None,
    log_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, int]:
    """Pull remote updates into SQLite and return statistics."""

    parsed_id = parse_spreadsheet_id(spreadsheet_id)
    if not parsed_id:
        raise SpreadsheetAccessError("Geçerli bir Sheet ID gerekli.")

    service = get_client(credential_path)
    _ensure_sheet_structure(service, parsed_id, worksheet_title)

    start = time.monotonic()
    with _connect(db_path) as conn:
        meta = _read_local_meta(conn)
    last_pull = _parse_timestamp(meta.get("last_pull_utc")) if meta else None

    remote_rows = _read_remote_rows(service, parsed_id, worksheet_title)
    changed: List[SheetRow] = []
    for row in remote_rows:
        updated_at = _parse_timestamp(row.values.get("UpdatedAt"))
        if last_pull is None or (updated_at and updated_at > last_pull):
            changed.append(row)

    applied = 0
    with _connect(db_path) as conn:
        previous_hashes = _load_previous_hashes(conn)
        for row in changed:
            payload = _sheet_row_to_db_payload(row.values)
            local_row = previous_hashes.get(row.row_id)
            if local_row is not None:
                # Retrieve current local row for conflict resolution
                local_cursor = conn.execute(
                    "SELECT item_id, rug_no, upc, roll_no, v_rug_no, v_collection, collection, "
                    "v_design, design, brand_name, ground, border, a_size, st_size, area, type, "
                    "rate, amount, shape, style, image_file_name, origin, retail, sp, msrp, cost, "
                    "qty, created_at, updated_at, version, status, location, consignment_id "
                    "FROM item WHERE item_id = ?",
                    (row.row_id,),
                )
                existing_row = local_cursor.fetchone()
                if existing_row:
                    local_sheet = _sqlite_row_to_sheet(existing_row)
                    winning = resolve_conflict(local_sheet.values, row.values)
                    payload = _sheet_row_to_db_payload(winning)
            db.upsert_item(payload)
            applied += 1
        _update_local_hash_state(conn, remote_rows)
        now_iso = datetime.utcnow().replace(microsecond=0).isoformat()
        _write_local_meta(conn, {"last_pull_utc": now_iso, "db_version": APP_VERSION})

    duration = time.monotonic() - start
    now_remote = datetime.utcnow().replace(microsecond=0).isoformat()
    _write_remote_meta(
        service,
        parsed_id,
        {"last_pull_utc": now_remote, "db_version": APP_VERSION},
    )
    _append_sync_log(
        service,
        parsed_id,
        direction="pull",
        action="delta",
        rows=applied,
        duration=duration,
        retries=0,
    )

    if log_callback:
        log_callback(f"{applied} satır güncellendi.")

    return {"applied": applied, "total_remote": len(remote_rows)}


def health_check(
    spreadsheet_id: str,
    credential_path: str,
    *,
    worksheet_title: str = DEFAULT_WORKSHEET_TITLE,
) -> Dict[str, Any]:
    """Execute a series of health checks returning a summary report."""

    parsed_id = parse_spreadsheet_id(spreadsheet_id)
    if not parsed_id:
        raise SpreadsheetAccessError("Geçerli bir Sheet ID gerekli.")

    service = get_client(credential_path)
    worksheet_id = _ensure_sheet_structure(service, parsed_id, worksheet_title)
    report = {
        "worksheet_id": worksheet_id,
        "headers": True,
        "meta": True,
        "logs": True,
    }
    try:
        remote_rows = _read_remote_rows(service, parsed_id, worksheet_title)
    except HttpError as exc:  # pragma: no cover - network interaction
        raise SpreadsheetAccessError(f"Sheets read başarısız: {exc}") from exc
    report["row_count"] = len(remote_rows)
    return report


def open_logs() -> str:
    """Return the path to the local synchronisation log file, creating it."""

    log_path = app_paths.logs_path("sheets-sync.log")
    if not log_path.exists():
        with contextlib.suppress(OSError):
            log_path.touch()
    return str(log_path)


__all__ = [
    "HEADERS",
    "SheetRow",
    "calc_hash",
    "chunked",
    "detect_local_deltas",
    "get_client",
    "is_api_available",
    "push",
    "pull",
    "health_check",
    "open_logs",
    "resolve_conflict",
    "SheetsSyncError",
    "MissingDependencyError",
    "CredentialsFileNotFoundError",
    "CredentialsFileInvalidError",
    "SpreadsheetAccessError",
]
