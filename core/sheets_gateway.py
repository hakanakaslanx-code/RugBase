"""Google Sheets gateway for RugBase inventory synchronisation.

This module provides a thin abstraction around the Google Sheets API that
implements all value conversion rules required by the current RugBase ↔
Sheets integration.  The public surface area intentionally mirrors the
operations required by the importer/exporter tools:

``get_rows``
    Fetch the full inventory worksheet, performing type conversion so that
    numeric values become ``float``/``int`` instances and empty strings are
    represented as ``None``.

``upsert_rows``
    Insert or update a batch of local rows, applying validation rules for the
    Status field and making sure numeric values are written as numbers.  The
    sheet header row is normalised before the data is uploaded.

``delete_rows``
    Remove rows identified by RugNo or SKU and rewrite the worksheet.

The implementation keeps the data transformation logic separate from the
Google client so that it can be unit tested without hitting the real API.
"""

from __future__ import annotations

from datetime import datetime, timezone
import getpass
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Set, Tuple

from core.google_credentials import ensure_service_account_file
from settings import DEFAULT_WORKSHEET_TITLE

logger = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:  # pragma: no cover - runtime guard
    service_account = None  # type: ignore[assignment]
    build = None  # type: ignore[assignment]

    class HttpError(Exception):  # type: ignore[override]
        """Fallback error type when googleapiclient is unavailable."""

    GOOGLE_API_AVAILABLE = False
else:  # pragma: no cover - simple assignment
    GOOGLE_API_AVAILABLE = True


SHEET_ID = "1n6_7L-8fPtQBN_QodxBXj3ZMzOPpMzdx8tpdRZZe5F8"
SHEET_NAME = DEFAULT_WORKSHEET_TITLE
REQUIRED_HEADERS: Tuple[str, ...] = (
    "RugNo",
    "Collection",
    "Design",
    "Ground",
    "Border",
    "ASize",
    "SSize",
    "Area",
    "Type",
    "Shape",
    "Style",
    "ImageFileName",
    "Origin",
    "Retail",
    "SP",
    "MSRP",
    "Cost",
    "Content",
    "LastUpdated",
    "Deleted",
)

FLOAT_COLUMNS = {"Area", "Retail", "SP", "MSRP", "Cost"}
INT_COLUMNS: Set[str] = set()
DATETIME_COLUMNS = {"LastUpdated"}
BOOL_COLUMNS = {"Deleted"}
UPSERT_KEYS = ("RugNo", "SKU")

MAX_BATCH_CELLS = 1_000
ROW_FETCH_CHUNK = 2_000


class SheetsGatewayError(Exception):
    """Base error raised when the Sheets gateway cannot complete an action."""


class MissingDependencyError(SheetsGatewayError):
    """Raised when google-api-python-client is not available."""


class CredentialsNotFoundError(SheetsGatewayError):
    """Raised when the expected service account file cannot be located."""


def is_api_available() -> bool:
    """Return ``True`` when the Google API client dependencies are present."""

    return GOOGLE_API_AVAILABLE


def _default_credentials_path() -> Path:
    username = os.environ.get("USERNAME") or os.environ.get("USER")
    if not username:
        try:
            username = getpass.getuser()
        except Exception:  # pragma: no cover - platform dependent fallback
            username = ""
    path = Path(f"C:/Users/{username}/AppData/Local/RugBase/credentials/service_account.json")
    return path


def _load_credentials(path: Optional[Path] = None):
    if not GOOGLE_API_AVAILABLE:
        raise MissingDependencyError(
            "google-api-python-client was not found. Google Sheets sync is disabled."
        )

    credentials_path = path or _default_credentials_path()
    if not credentials_path.exists():
        raise CredentialsNotFoundError(str(credentials_path))

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    payload = ensure_service_account_file(credentials_path)
    return service_account.Credentials.from_service_account_info(payload, scopes=scopes)


def _build_service(credentials=None):
    if not GOOGLE_API_AVAILABLE:
        raise MissingDependencyError(
            "google-api-python-client was not found. Google Sheets sync is disabled."
        )
    if credentials is None:
        credentials = _load_credentials()
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def build_service_from_file(path: str):
    """Construct a Sheets service using credentials from ``path``."""

    credentials = _load_credentials(Path(path))
    return _build_service(credentials)


def _column_letter(index: int) -> str:
    if index < 1:
        raise ValueError("Column index must be >= 1")
    letters: List[str] = []
    while index:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def _a1_range(worksheet_title: str, range_spec: str) -> str:
    title = worksheet_title or SHEET_NAME
    if "'" in title:
        title = title.replace("'", "''")
    return f"'{title}'!{range_spec}"


def _normalise_headers(existing: Sequence[str]) -> List[str]:
    extras: List[str] = []
    seen = set()
    for header in existing:
        header = (header or "").strip()
        if not header:
            continue
        if header in REQUIRED_HEADERS:
            continue
        if header in seen:
            continue
        extras.append(header)
        seen.add(header)
    return list(REQUIRED_HEADERS) + extras


def _header_map(headers: Sequence[str]) -> Dict[str, int]:
    return {header: index for index, header in enumerate(headers)}


def _parse_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Cannot convert {value!r} to float")


def _parse_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        raise ValueError(f"Cannot convert {value!r} to int")


def _parse_datetime(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value.astimezone(timezone.utc)
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"Cannot parse datetime value {value!r}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _coerce_from_sheet(header: str, value: Any) -> Any:
    if value == "":
        return None
    if header in BOOL_COLUMNS:
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "y"}:
                return True
            if lowered in {"0", "false", "no", "n"}:
                return False
        return bool(value)
    if header in DATETIME_COLUMNS:
        try:
            return _parse_datetime(value)
        except ValueError:
            return str(value)
    if header in FLOAT_COLUMNS:
        try:
            return float(value)
        except (TypeError, ValueError):
            return value
    if header in INT_COLUMNS:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return value
    return value


def _coerce_for_sheet(header: str, value: Any) -> Any:
    if header in FLOAT_COLUMNS:
        parsed = _parse_float(value)
        return "" if parsed is None else parsed
    if header in INT_COLUMNS:
        parsed = _parse_int(value)
        return "" if parsed is None else parsed
    if header in DATETIME_COLUMNS:
        parsed = _parse_datetime(value)
        return "" if parsed is None else parsed
    if header in BOOL_COLUMNS:
        if value in (None, ""):
            return ""
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "y"}:
                return True
            if lowered in {"0", "false", "no", "n"}:
                return False
        return bool(value)
    if value is None:
        return ""
    return value


def _ensure_header_row(service, spreadsheet_id: str, worksheet_title: str = SHEET_NAME) -> List[str]:
    request = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=_a1_range(worksheet_title, "1:1"))
    )
    result = request.execute()
    existing = result.get("values", []) if isinstance(result, dict) else []
    header_row = existing[0] if existing else []
    headers = _normalise_headers(header_row)

    if header_row != headers:
        value_range = {
            "range": _a1_range(worksheet_title, f"A1:{_column_letter(len(headers))}1"),
            "values": [list(headers)],
        }
        (
            service.spreadsheets()
            .values()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"valueInputOption": "RAW", "data": [value_range]},
            )
            .execute()
        )
    return headers


def _fetch_values(
    service,
    spreadsheet_id: str,
    headers: Sequence[str],
    worksheet_title: str = SHEET_NAME,
) -> List[List[Any]]:
    end_column = _column_letter(len(headers))
    rows: List[List[Any]] = []
    start_row = 2
    while True:
        end_row = start_row + ROW_FETCH_CHUNK - 1
        request = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=_a1_range(worksheet_title, f"A{start_row}:{end_column}{end_row}"),
            )
        )
        result = request.execute()
        values = result.get("values", []) if isinstance(result, dict) else []
        if not values:
            break
        rows.extend(list(map(list, values)))
        if len(values) < ROW_FETCH_CHUNK:
            break
        start_row += ROW_FETCH_CHUNK
    return rows


def _rows_from_values(headers: Sequence[str], values: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw in values:
        row: Dict[str, Any] = {}
        for index, header in enumerate(headers):
            cell = raw[index] if index < len(raw) else ""
            row[header] = _coerce_from_sheet(header, cell)
        rows.append(row)
    return rows


def _key_for_row(row: Mapping[str, Any]) -> Tuple[str, str]:
    for key in UPSERT_KEYS:
        value = row.get(key)
        if value not in (None, ""):
            return key, str(value)
    raise SheetsGatewayError("Row is missing both RugNo and SKU")


def _index_rows(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[Tuple[str, str], int]]:
    index: Dict[Tuple[str, str], int] = {}
    for position, row in enumerate(rows):
        for key in UPSERT_KEYS:
            value = row.get(key)
            if value in (None, ""):
                continue
            index[(key, str(value))] = position
    return rows, index


def _format_rows_for_sheet(headers: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> List[List[Any]]:
    formatted: List[List[Any]] = []
    for row in rows:
        formatted_row = []
        for header in headers:
            value = row.get(header)
            formatted_row.append(_coerce_for_sheet(header, value))
        formatted.append(formatted_row)
    return formatted


def _chunk_rows(
    matrix: Sequence[List[Any]], max_cells: int = MAX_BATCH_CELLS
) -> Iterator[Tuple[int, List[List[Any]]]]:
    if not matrix:
        return
    column_count = max(len(row) for row in matrix)
    if column_count == 0:
        column_count = 1
    rows_per_chunk = max(1, max_cells // column_count)
    for start in range(0, len(matrix), rows_per_chunk):
        yield start, [list(row) for row in matrix[start : start + rows_per_chunk]]


def _write_sheet(
    service,
    spreadsheet_id: str,
    headers: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
    worksheet_title: str = SHEET_NAME,
) -> None:
    matrix = [list(headers)] + _format_rows_for_sheet(headers, rows)
    end_column = _column_letter(len(headers))
    data: List[Dict[str, Any]] = []
    for start, chunk in _chunk_rows(matrix):
        start_row = start + 1
        end_row = start_row + len(chunk) - 1
        data.append(
            {
                "range": _a1_range(worksheet_title, f"A{start_row}:{end_column}{end_row}"),
                "values": chunk,
            }
        )

    if not data:
        data.append(
            {
                "range": _a1_range(worksheet_title, f"A1:{end_column}1"),
                "values": [list(headers)],
            }
        )

    (
        service.spreadsheets()
        .values()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": data},
        )
        .execute()
    )


def get_rows(
    service=None,
    spreadsheet_id: str = SHEET_ID,
    *,
    worksheet_title: str = SHEET_NAME,
) -> List[Dict[str, Any]]:
    """Return all rows from the inventory sheet with type conversion applied."""

    if service is None:
        service = _build_service()
    headers = _ensure_header_row(service, spreadsheet_id, worksheet_title)
    values = _fetch_values(service, spreadsheet_id, headers, worksheet_title)
    return _rows_from_values(headers, values)


def upsert_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    service=None,
    spreadsheet_id: str = SHEET_ID,
    worksheet_title: str = SHEET_NAME,
) -> None:
    """Insert or update rows on the inventory sheet."""

    if not rows:
        return
    if service is None:
        service = _build_service()
    headers = _ensure_header_row(service, spreadsheet_id, worksheet_title)
    existing_values = _fetch_values(service, spreadsheet_id, headers, worksheet_title)
    current_rows = _rows_from_values(headers, existing_values)
    current_rows, index = _index_rows(current_rows)

    for incoming in rows:
        key_name, key_value = _key_for_row(incoming)
        payload: Dict[str, Any]
        position = index.get((key_name, key_value))
        if position is not None:
            payload = current_rows[position]
        else:
            payload = {header: None for header in headers}
            current_rows.append(payload)
            position = len(current_rows) - 1
        for header, value in incoming.items():
            payload[header] = value
        index[(key_name, key_value)] = position
        for other_key in UPSERT_KEYS:
            other_value = incoming.get(other_key)
            if other_value in (None, ""):
                continue
            index[(other_key, str(other_value))] = position

    _write_sheet(service, spreadsheet_id, headers, current_rows, worksheet_title)


def delete_rows(
    keys: Iterable[str],
    *,
    service=None,
    spreadsheet_id: str = SHEET_ID,
    worksheet_title: str = SHEET_NAME,
) -> None:
    """Remove rows identified by RugNo or SKU from the worksheet."""

    key_set = {key for key in keys if key}
    if not key_set:
        return
    if service is None:
        service = _build_service()
    headers = _ensure_header_row(service, spreadsheet_id, worksheet_title)
    existing_values = _fetch_values(service, spreadsheet_id, headers, worksheet_title)
    current_rows = _rows_from_values(headers, existing_values)

    filtered: List[Dict[str, Any]] = []
    for row in current_rows:
        identifiers = {str(row.get(key)) for key in UPSERT_KEYS if row.get(key) not in (None, "")}
        if identifiers & key_set:
            continue
        filtered.append(row)

    _write_sheet(service, spreadsheet_id, headers, filtered, worksheet_title)


__all__ = [
    "SHEET_ID",
    "SHEET_NAME",
    "REQUIRED_HEADERS",
    "SheetsGatewayError",
    "MissingDependencyError",
    "CredentialsNotFoundError",
    "is_api_available",
    "build_service_from_file",
    "get_rows",
    "upsert_rows",
    "delete_rows",
]

