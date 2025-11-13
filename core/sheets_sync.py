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
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

import db
from core import app_paths
from core.google_credentials import CredentialsFileInvalidError, ensure_service_account_file
from core.version import __version__ as APP_VERSION
from core.conflicts import record as record_conflict
from core.offline_queue import OutboxQueue

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
# Module level helpers/state
# ---------------------------------------------------------------------------
_OUTBOX = OutboxQueue()
_DEBOUNCE_WINDOW = 3.0
_DEBOUNCE_LOCK = threading.Lock()
_DEBOUNCE_STATE: Dict[str, float] = {}


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
    "SoldAt",
    "CustomerId",
    "SalePrice",
    "SaleNote",
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
    "SoldAt": "sold_at",
    "CustomerId": "customer_id",
    "SalePrice": "sale_price",
    "SaleNote": "sale_note",
    "UpdatedAt": "updated_at",
    "Deleted": None,
}

CUSTOMER_SHEET_TITLE = "Customers"
CUSTOMER_HEADERS: Tuple[str, ...] = (
    "Id",
    "FullName",
    "Phone",
    "Email",
    "Address",
    "City",
    "State",
    "Zip",
    "Notes",
    "CreatedAt",
    "UpdatedAt",
)

CUSTOMER_FIELD_MAP: Mapping[str, str] = {
    "Id": "id",
    "FullName": "full_name",
    "Phone": "phone",
    "Email": "email",
    "Address": "address",
    "City": "city",
    "State": "state",
    "Zip": "zip",
    "Notes": "notes",
    "CreatedAt": "created_at",
    "UpdatedAt": "updated_at",
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


class SpreadsheetAccessError(SheetsSyncError):
    """Raised when the Google Sheets API returns an error."""


class SheetsPermissionError(SheetsSyncError):
    """Raised when the service account lacks sufficient permissions."""


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


def _debounce_row(row_id: str) -> bool:
    now = time.monotonic()
    with _DEBOUNCE_LOCK:
        last = _DEBOUNCE_STATE.get(row_id)
        if last is not None and now - last < _DEBOUNCE_WINDOW:
            return True
        _DEBOUNCE_STATE[row_id] = now
    return False


def _queue_failed_rows(rows: Iterable[SheetRow]) -> None:
    entries = []
    for row in rows:
        if not row.row_id:
            continue
        entries.append({"row_id": row.row_id})
    if entries:
        _OUTBOX.append(entries)


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
            "google-api-python-client was not found. Please install the dependencies."
        )


def get_client(credentials_path: str, payload: Optional[Mapping[str, object]] = None):
    """Return an authenticated Sheets API client using the service account."""

    _require_api()
    path = Path(os.path.expanduser(credentials_path)).resolve()
    if not path.exists():
        raise CredentialsFileNotFoundError(f"Credentials file not found: {path}")

    try:
        data = payload or ensure_service_account_file(path)
        credentials = service_account.Credentials.from_service_account_info(  # type: ignore[union-attr]
            data, scopes=SCOPES
        )
    except ValueError as exc:  # pragma: no cover - invalid key file
        raise CredentialsFileInvalidError(str(exc) or "JSON eksik alanlar: private_key") from exc
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)  # type: ignore[call-arg]


_SIMPLE_TITLE_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _quote_title(title: str) -> str:
    """Return a worksheet title safely formatted for A1 notation."""

    normalised = (title or "").strip()
    if not normalised:
        return "''"

    if _SIMPLE_TITLE_RE.fullmatch(normalised):
        return normalised

    escaped = normalised.replace("'", "''")
    return f"'{escaped}'"


def _a1_range(title: str, range_spec: str) -> str:
    return f"{_quote_title(title)}!{range_spec}"


def _http_status(exc: HttpError) -> int:
    status = getattr(exc, "status_code", None)
    if status is not None:
        try:
            return int(status)
        except (TypeError, ValueError):
            return 0
    resp = getattr(exc, "resp", None)
    if resp is not None:
        try:
            return int(getattr(resp, "status", 0))
        except (TypeError, ValueError):
            return 0
    return 0


def _resolve_worksheet(
    metadata: Mapping[str, Any],
    worksheet_title: str,
    sheet_gid: str,
) -> Tuple[str, Optional[int]]:
    sheets = metadata.get("sheets", []) if isinstance(metadata, Mapping) else []
    candidates: List[Tuple[str, Optional[int]]] = []
    for sheet in sheets:
        if not isinstance(sheet, Mapping):
            continue
        props = sheet.get("properties", {})
        if not isinstance(props, Mapping):
            continue
        title = props.get("title")
        sheet_id = props.get("sheetId")
        if isinstance(title, str):
            candidates.append((title, sheet_id if isinstance(sheet_id, int) else None))

    normalised_title = (worksheet_title or "").strip()
    gid_candidate = str(sheet_gid or "").strip()

    if normalised_title:
        lower = normalised_title.lower()
        for title, sheet_id in candidates:
            if title.lower() == lower:
                return title, sheet_id

    if gid_candidate:
        for title, sheet_id in candidates:
            if sheet_id is not None and str(sheet_id) == gid_candidate:
                return title, sheet_id

    if normalised_title:
        return normalised_title, None

    if candidates:
        return candidates[0]

    raise SpreadsheetAccessError("Worksheet not found in spreadsheet metadata.")


def _read_header_row(service, spreadsheet_id: str, worksheet_title: str) -> List[str]:
    primary_range = _a1_range(worksheet_title, "1:1")
    try:
        payload = _values_batch_get(service, spreadsheet_id, [primary_range])
    except HttpError:
        fallback_range = _a1_range(worksheet_title, "A1:Z1")
        payload = _values_batch_get(service, spreadsheet_id, [fallback_range])
    value_ranges = payload.get("valueRanges", [])
    if not value_ranges:
        return []
    rows = value_ranges[0].get("values", [])
    if not rows:
        return []
    return rows[0]


def _perform_write_check(
    service,
    spreadsheet_id: str,
    worksheet_title: str,
    account_email: str,
) -> str:
    test_range = _a1_range(worksheet_title, "Z1")
    marker = f"rb-health-{int(time.time() * 1000)}"
    original_value = ""
    changed = False

    try:
        existing = _values_batch_get(service, spreadsheet_id, [test_range])
        values = existing.get("valueRanges", [{}])[0].get("values", [])
        if values and values[0]:
            original_value = str(values[0][0])
        data = [{"range": test_range, "values": [[marker]]}]
        _values_batch_update(service, spreadsheet_id, data)
        changed = True
        confirm = _values_batch_get(service, spreadsheet_id, [test_range])
        confirm_values = confirm.get("valueRanges", [{}])[0].get("values", [])
        confirmed = bool(confirm_values and confirm_values[0] and confirm_values[0][0] == marker)
    except HttpError as exc:  # pragma: no cover - network interaction
        status = _http_status(exc)
        if status == 403:
            raise SheetsPermissionError(
                f"Service account edit yetkisi yok (mail: {account_email})."
            ) from exc
        raise SpreadsheetAccessError(f"Sheets write failed: {exc}") from exc
    finally:
        if changed:
            restore_value = original_value if original_value is not None else ""
            try:
                _values_batch_update(
                    service,
                    spreadsheet_id,
                    [{"range": test_range, "values": [[restore_value]]}],
                )
            except Exception:  # pragma: no cover - best effort
                logger.debug("Failed to restore test cell after health check", exc_info=True)

    if changed and confirmed:
        logger.info("Sheets write OK")
        return "ok"
    if changed:
        return "mismatch"
    return "skipped"


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
        "qty, created_at, updated_at, version, status, location, consignment_id, "
        "sold_at, customer_id, sale_price, sale_note "
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
                "Sheets API %s error (%s). Retrying in %ss (%d/%d)",
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


def _values_clear(service, spreadsheet_id: str, range_spec: str) -> None:
    request = service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=range_spec,
        body={},
    )
    _call_with_retry(request.execute, "values.clear")


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
    customer_sheet_id: Optional[int] = None

    for sheet in sheets:
        properties = sheet.get("properties", {})
        if properties.get("title") == worksheet_title:
            worksheet_id = properties.get("sheetId")
        if properties.get("title") == META_SHEET_TITLE:
            meta_id = properties.get("sheetId")
        if properties.get("title") == LOG_SHEET_TITLE:
            log_id = properties.get("sheetId")
        if properties.get("title") == CUSTOMER_SHEET_TITLE:
            customer_sheet_id = properties.get("sheetId")

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
    if customer_sheet_id is None:
        requests.append(
            {
                "addSheet": {
                    "properties": {
                        "title": CUSTOMER_SHEET_TITLE,
                        "gridProperties": {
                            "rowCount": 2,
                            "columnCount": len(CUSTOMER_HEADERS),
                        },
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
            if properties.get("title") == CUSTOMER_SHEET_TITLE:
                customer_sheet_id = properties.get("sheetId")

    if worksheet_id is None:
        raise SpreadsheetAccessError("Worksheet could not be found or created.")

    # Ensure headers present
    header_range = _a1_range(worksheet_title, f"A1:{_column_a1(len(HEADERS) - 1)}1")
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

    customer_header_range = _a1_range(
        CUSTOMER_SHEET_TITLE, f"A1:{_column_a1(len(CUSTOMER_HEADERS) - 1)}1"
    )
    current_customer_headers = _values_batch_get(
        service,
        spreadsheet_id,
        [customer_header_range],
    )
    customer_values = current_customer_headers.get("valueRanges", [{}])[0].get("values", [])
    if not customer_values or customer_values[0] != list(CUSTOMER_HEADERS):
        _values_batch_update(
            service,
            spreadsheet_id,
            [
                {
                    "range": customer_header_range,
                    "values": [list(CUSTOMER_HEADERS)],
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
    meta_header_range = _a1_range(META_SHEET_TITLE, "A1:B1")
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
    log_header_range = _a1_range(LOG_SHEET_TITLE, "A1:F1")
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


def _sync_customers_sheet(
    service,
    spreadsheet_id: str,
    customers: Sequence[Mapping[str, Any]],
    *,
    log_callback: Optional[Callable[[str], None]] = None,
) -> int:
    clear_range = _a1_range(
        CUSTOMER_SHEET_TITLE, f"A2:{_column_a1(len(CUSTOMER_HEADERS) - 1)}"
    )
    _values_clear(service, spreadsheet_id, clear_range)

    if not customers:
        if log_callback:
            log_callback("Customers sheet cleared (0 rows).")
        return 0

    rows: List[List[str]] = []
    for record in customers:
        row: List[str] = []
        for header in CUSTOMER_HEADERS:
            key = CUSTOMER_FIELD_MAP[header]
            value = record.get(key)
            row.append("" if value is None else str(value))
        rows.append(row)

    start_range = _a1_range(CUSTOMER_SHEET_TITLE, "A2")
    _values_batch_update(
        service,
        spreadsheet_id,
        [
            {
                "range": start_range,
                "values": rows,
            }
        ],
    )
    if log_callback:
        log_callback(f"Customers sheet synchronized ({len(rows)} rows).")
    return len(rows)


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
    log_sheet_range = _a1_range(LOG_SHEET_TITLE, "A:A")
    existing = _values_batch_get(service, spreadsheet_id, [log_sheet_range])
    entries = existing.get("valueRanges", [{}])[0].get("values", [])
    next_row = len(entries) + 1
    data = [
        {
            "range": _a1_range(LOG_SHEET_TITLE, f"A{next_row}:F{next_row}"),
            "values": [[timestamp, direction, action, str(rows), f"{duration:.3f}", str(retries)]],
        }
    ]
    _values_batch_update(service, spreadsheet_id, data)


def _read_remote_rows(
    service,
    spreadsheet_id: str,
    worksheet_title: str,
) -> List[SheetRow]:
    range_a1 = _a1_range(worksheet_title, f"A1:{_column_a1(len(HEADERS) - 1)}")
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
    existing = _values_batch_get(service, spreadsheet_id, [_a1_range(META_SHEET_TITLE, "A:B")])
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
        {"range": _a1_range(META_SHEET_TITLE, f"A1:B{len(ordered)}"), "values": ordered}
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
    """Merge conflicting rows field-by-field using UpdatedAt timestamps."""

    backup_dir = app_paths.ensure_directory(app_paths.BACKUP_DIR)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    row_id = remote_row.get("RowID") or local_row.get("RowID") or "unknown"
    local_ts = _parse_timestamp(local_row.get("UpdatedAt"))
    remote_ts = _parse_timestamp(remote_row.get("UpdatedAt"))

    merged = dict(remote_row)
    field_diffs: Dict[str, Tuple[str, str]] = {}

    for header in HEADERS:
        if header in {"RowID", "Hash", "UpdatedAt"}:
            continue
        local_value = local_row.get(header)
        remote_value = remote_row.get(header)
        if str(local_value) == str(remote_value):
            merged[header] = remote_value
            continue
        if local_ts and remote_ts:
            if local_ts > remote_ts:
                merged[header] = local_value
                continue
            if remote_ts > local_ts:
                merged[header] = remote_value
                continue
        elif local_ts and not remote_ts:
            merged[header] = local_value
            continue
        elif remote_ts and not local_ts:
            merged[header] = remote_value
            continue

        # Equal or missing timestamps – favour remote but record the difference
        merged[header] = remote_value
        field_diffs[header] = (str(local_value or ""), str(remote_value or ""))

    chosen_ts: Optional[datetime] = None
    if local_ts and remote_ts:
        chosen_ts = max(local_ts, remote_ts)
    else:
        chosen_ts = local_ts or remote_ts
    if field_diffs:
        chosen_ts = datetime.utcnow().replace(microsecond=0, tzinfo=timezone.utc)

    if chosen_ts:
        merged["UpdatedAt"] = chosen_ts.isoformat().replace("+00:00", "Z")

    def _backup(row: Mapping[str, Any], suffix: str) -> None:
        path = backup_dir / f"{backup_prefix}-{row_id}-{suffix}-{timestamp}.bak.json"
        try:
            with path.open("w", encoding="utf-8") as handle:
                json.dump(row, handle, ensure_ascii=False, indent=2)
        except OSError:  # pragma: no cover - filesystem guard
            logger.warning("Conflict backup could not be written: %s", path, exc_info=True)

    if field_diffs:
        _backup(local_row, "local")
        _backup(remote_row, "remote")
        record_conflict(row_id, field_diffs, context={"strategy": "field_merge"})

    return merged


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
        "sold_at": row.get("SoldAt"),
        "customer_id": row.get("CustomerId"),
        "sale_price": row.get("SalePrice"),
        "sale_note": row.get("SaleNote"),
    }
    if str(row.get("Deleted", "")).upper() == "TRUE":
        payload["status"] = "archived"
        payload["qty"] = 0
    return payload


def _flush_outbox(
    service,
    parsed_id: str,
    worksheet_title: str,
    *,
    db_path: Optional[str],
    log_callback: Optional[Callable[[str], None]] = None,
) -> int:
    if not _OUTBOX.path.exists():
        return 0

    remote_rows = _read_remote_rows(service, parsed_id, worksheet_title)
    remote_index: Dict[str, SheetRow] = {row.row_id: row for row in remote_rows}
    next_row_index = max((row.row_index or 1 for row in remote_rows), default=1) + 1

    with _connect(db_path) as conn:
        local_index = {row.row_id: row for row in _fetch_local_rows(conn)}

    def _replay(entry: Mapping[str, object]) -> None:
        nonlocal next_row_index
        row_id = str(entry.get("row_id") or "")
        if not row_id:
            return
        row = local_index.get(row_id)
        if row is None:
            return
        target_index: Optional[int] = None
        existing = remote_index.get(row_id)
        if existing and existing.row_index is not None:
            target_index = existing.row_index
        if target_index is None:
            target_index = next_row_index
            next_row_index += 1
            remote_index[row_id] = SheetRow(
                row_id=row.row_id,
                values=dict(row.values),
                hash=row.hash,
                row_index=target_index,
            )
        a1_range = _a1_range(worksheet_title, _sheet_range(target_index))
        _values_batch_update(
            service,
            parsed_id,
            [{"range": a1_range, "values": [row.as_list()]}],
        )
        if log_callback:
            log_callback(f"Outbox entry processed: {row_id}")

    processed = _OUTBOX.drain(_replay)
    return processed


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
        raise SpreadsheetAccessError("A valid Sheet ID is required.")

    service = get_client(credential_path)
    worksheet_id = _ensure_sheet_structure(service, parsed_id, worksheet_title)

    try:
        customer_rows = db.fetch_customers_for_sheet()
    except Exception as exc:  # pragma: no cover - defensive guard
        raise SpreadsheetAccessError(f"Customer data could not be read: {exc}") from exc

    try:
        customer_synced = _sync_customers_sheet(
            service,
            parsed_id,
            customer_rows,
            log_callback=log_callback,
        )
    except Exception as exc:  # pragma: no cover - network/IO guard
        raise SpreadsheetAccessError(f"Customers sheet could not be updated: {exc}") from exc

    processed_outbox = 0
    try:
        processed_outbox = _flush_outbox(
            service,
            parsed_id,
            worksheet_title,
            db_path=db_path,
            log_callback=log_callback,
        )
    except Exception as exc:  # pragma: no cover - network/IO guard
        raise SpreadsheetAccessError(f"Outbox queue could not be sent: {exc}") from exc
    if processed_outbox and log_callback:
        log_callback(f"Queued {processed_outbox} rows uploaded.")

    start = time.monotonic()
    with _connect(db_path) as conn:
        previous_hashes = _load_previous_hashes(conn)
        local_rows = _fetch_local_rows(conn)

    detected_new_rows, changed_rows = detect_local_deltas(local_rows, previous_hashes)

    remote_rows = _read_remote_rows(service, parsed_id, worksheet_title)
    remote_index: Dict[str, SheetRow] = {row.row_id: row for row in remote_rows}

    new_rows: List[SheetRow] = []
    for row in detected_new_rows:
        if _debounce_row(row.row_id):
            if log_callback:
                log_callback(f"Entry {row.row_id} delayed due to debounce.")
            continue
        new_rows.append(row)

    updates: List[Tuple[int, SheetRow]] = []
    for row in changed_rows:
        if _debounce_row(row.row_id):
            if log_callback:
                log_callback(f"Entry {row.row_id} skipped due to debounce.")
            continue
        remote = remote_index.get(row.row_id)
        target_index = remote.row_index if remote else row.row_index
        if target_index is None and remote:
            target_index = remote.row_index
        if target_index is None:
            new_rows.append(row)
        else:
            row.row_index = target_index
            updates.append((target_index, row))

    # Determine append start row
    next_row_index = max((row.row_index or 1 for row in remote_rows), default=1) + 1

    total_written = 0
    total_retries = 0

    if updates:
        batches: List[List[Tuple[int, SheetRow]]] = [list(chunk) for chunk in chunked(updates, MAX_BATCH_ROWS)]
        for batch in batches:
            data = []
            for row_index, row in batch:
                a1_range = _a1_range(worksheet_title, _sheet_range(row_index))
                data.append({"range": a1_range, "values": [row.as_list()]})
            try:
                _, retries = _values_batch_update(service, parsed_id, data)
            except Exception as exc:  # pragma: no cover - network/IO guard
                _queue_failed_rows(row for _, row in batch)
                raise SpreadsheetAccessError(
                    f"Sheets update failed: {exc}"
                ) from exc
            total_written += len(batch)
            total_retries += retries
            if log_callback:
                log_callback(
                    f"{len(batch)} rows updated (retry={retries})."
                )

    if new_rows:
        batches = [list(chunk) for chunk in chunked(new_rows, MAX_BATCH_ROWS)]
        for batch in batches:
            data = []
            for row in batch:
                row.row_index = next_row_index
                data.append(
                    {
                        "range": _a1_range(worksheet_title, _sheet_range(next_row_index)),
                        "values": [row.as_list()],
                    }
                )
                next_row_index += 1
            try:
                _, retries = _values_batch_update(service, parsed_id, data)
            except Exception as exc:  # pragma: no cover - network/IO guard
                _queue_failed_rows(batch)
                raise SpreadsheetAccessError(
                    f"Sheets insert failed: {exc}"
                ) from exc
            total_written += len(batch)
            total_retries += retries
            if log_callback:
                log_callback(
                    f"{len(batch)} new rows added (retry={retries})."
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
        "customers": customer_synced,
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
        raise SpreadsheetAccessError("A valid Sheet ID is required.")

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
                    "qty, created_at, updated_at, version, status, location, consignment_id, "
                    "sold_at, customer_id, sale_price, sale_note "
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
        log_callback(f"{applied} rows updated.")

    return {"applied": applied, "total_remote": len(remote_rows)}


def latest_remote_updated_at(
    service,
    spreadsheet_id: str,
    worksheet_title: str = DEFAULT_WORKSHEET_TITLE,
) -> Optional[str]:
    parsed_id = parse_spreadsheet_id(spreadsheet_id)
    if not parsed_id:
        raise SpreadsheetAccessError("A valid Sheet ID is required.")

    try:
        updated_index = HEADERS.index("UpdatedAt") + 1
    except ValueError:
        return None
    column = _column_a1(updated_index)
    range_spec = _a1_range(worksheet_title, f"{column}2:{column}")

    response = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=parsed_id, range=range_spec)
        .execute()
    )

    values = response.get("values", []) if isinstance(response, dict) else []
    latest: Optional[datetime] = None
    for row in values:
        if not row:
            continue
        candidate = _parse_timestamp(row[0])
        if candidate and (latest is None or candidate > latest):
            latest = candidate
    if latest is None:
        return None
    return latest.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def health_check(
    spreadsheet_id: str,
    credential_path: str,
    *,
    worksheet_title: str = DEFAULT_WORKSHEET_TITLE,
    sheet_gid: str = "",
    service_account_email: str = "",
) -> Dict[str, Any]:
    """Execute a series of health checks returning a summary report."""

    parsed_id = parse_spreadsheet_id(spreadsheet_id)
    if not parsed_id:
        raise SpreadsheetAccessError("A valid Sheet ID is required.")

    path = Path(os.path.expanduser(credential_path)).resolve()
    if not path.exists():
        raise CredentialsFileNotFoundError(f"Credentials file not found: {path}")

    payload = ensure_service_account_file(path)
    raw_email = payload.get("client_email")
    if isinstance(raw_email, str) and raw_email.strip():
        account_email = raw_email.strip()
    elif service_account_email and service_account_email.strip():
        account_email = service_account_email.strip()
    else:
        account_email = ""

    service = get_client(credential_path, payload)

    try:
        metadata = _spreadsheet_get(service, parsed_id)
    except HttpError as exc:  # pragma: no cover - network interaction
        status = _http_status(exc)
        if status == 403:
            raise SheetsPermissionError(
                f"Service account edit yetkisi yok (mail: {account_email})."
            ) from exc
        raise SpreadsheetAccessError(f"Sheets read failed: {exc}") from exc

    resolved_title, resolved_id = _resolve_worksheet(metadata, worksheet_title, sheet_gid)
    worksheet_id = _ensure_sheet_structure(service, parsed_id, resolved_title)
    if worksheet_id is None and resolved_id is not None:
        worksheet_id = resolved_id

    header_row = _read_header_row(service, parsed_id, resolved_title)

    try:
        remote_rows = _read_remote_rows(service, parsed_id, resolved_title)
    except HttpError as exc:  # pragma: no cover - network interaction
        raise SpreadsheetAccessError(f"Sheets read failed: {exc}") from exc

    write_result = _perform_write_check(service, parsed_id, resolved_title, account_email or service_account_email)

    report = {
        "worksheet_id": worksheet_id,
        "headers": bool(header_row),
        "row_count": len(remote_rows),
        "resolved_title": resolved_title,
        "write_check": write_result,
    }

    logger.info(
        "Health check: headers=%s row_count=%s worksheet_id=%s resolved_title=%s write_check=%s",
        report["headers"],
        report["row_count"],
        report["worksheet_id"],
        report["resolved_title"],
        report["write_check"],
    )

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
    "SheetsPermissionError",
    "latest_remote_updated_at",
]
