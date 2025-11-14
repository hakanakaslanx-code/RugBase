"""Google Sheets client helpers with robust A1 range handling.

This module centralises all direct interactions with the Google Sheets API
used by RugBase.  It provides a small, well defined surface area that the rest
of the application can rely on without needing to know about HTTP requests or
googleapiclient internals.  The implementation focuses on three goals:

* Normalising worksheet titles and A1 ranges so that "Unable to parse range"
  errors are eliminated.  Titles are always quoted according to the Sheets
  specification and column references are calculated with a dedicated helper.
* Performing bulk operations.  ``fetch_tabs`` reads multiple worksheets in a
  single ``batchGet`` request while ``update_tabs`` writes complete tables with
  ``batchUpdate``.  This keeps network chatter to a minimum which is crucial
  for large workbooks.
* Providing a clean failure surface.  All public entry points raise subclasses
  of :class:`SheetsClientError`, making it straightforward for callers to
  communicate meaningful status messages to the user interface.

The module intentionally keeps its public API very small.  Higher level data
managers (such as :mod:`db`) are expected to perform tasks like diffing or
validation, while this layer focuses purely on HTTP interactions and data
shape conversions.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableSequence, Optional, Sequence

from core.google_credentials import CredentialsFileInvalidError, ensure_service_account_file
from core.excel_service import ExcelService

try:  # pragma: no cover - optional dependency
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:  # pragma: no cover - runtime guard for tests/offline envs
    service_account = None  # type: ignore[assignment]
    build = None  # type: ignore[assignment]

    class HttpError(Exception):
        """Fallback error type raised when googleapiclient is unavailable."""

    GOOGLE_API_AVAILABLE = False
else:  # pragma: no cover - trivial attribute set
    GOOGLE_API_AVAILABLE = True


SCOPES: Sequence[str] = ("https://www.googleapis.com/auth/spreadsheets",)


@dataclass(slots=True)
class SheetTabData:
    """Container holding the raw values for a worksheet tab."""

    title: str
    headers: List[str]
    rows: List[List[str]]


class SheetsClientError(RuntimeError):
    """Base error raised for Sheets API failures."""


class SheetsDependencyError(SheetsClientError):
    """Raised when the Google API client dependencies are missing."""


class SheetsCredentialsError(SheetsClientError):
    """Raised when the provided credential file is invalid or missing."""


class SheetsApiResponseError(SheetsClientError):
    """Raised when the Google API returns an error response."""


def _require_google_api() -> None:
    if not GOOGLE_API_AVAILABLE:
        raise SheetsDependencyError(
            "google-api-python-client was not found. Google Sheets sync is disabled."
        )


def _normalise_title(title: str) -> str:
    """Return a worksheet title quoted according to A1 notation rules."""

    safe = (title or "").strip()
    if not safe:
        raise SheetsClientError("Worksheet title must be configured in Sync Settings.")
    safe = safe.replace("'", "''")
    return f"'{safe}'"


def _column_letter(index: int) -> str:
    if index < 1:
        raise ValueError("Column index must be >= 1")
    letters: MutableSequence[str] = []
    while index:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def _column_count(columns: int) -> int:
    """Clamp ``columns`` to the valid range for A1 helpers."""

    return max(1, columns)


def a1_headers_range(title: str, *, columns: int) -> str:
    """Return an A1 range covering the header row for ``title``."""

    column_count = _column_count(columns)
    last_column = _column_letter(column_count)
    return f"{_normalise_title(title)}!A1:{last_column}1"


def a1_full_column_range(title: str, *, columns: int = 26) -> str:
    """Return an A1 range spanning all rows for ``columns`` columns."""

    column_count = _column_count(columns)
    last_column = _column_letter(column_count)
    return f"{_normalise_title(title)}!A1:{last_column}"


def a1_row_range(title: str, row_index: int, *, columns: int) -> str:
    """Return an A1 range covering ``row_index`` for ``title``."""

    if row_index < 1:
        raise ValueError("Row index must be >= 1")
    column_count = _column_count(columns)
    last_column = _column_letter(column_count)
    return f"{_normalise_title(title)}!A{row_index}:{last_column}{row_index}"


def _range_for_title(title: str, *, columns: int = 26) -> str:
    """Return an A1 range covering ``columns`` columns for ``title``."""

    return a1_full_column_range(title, columns=columns)


def _build_service(path: Path):
    _require_google_api()
    try:
        payload = ensure_service_account_file(path)
    except CredentialsFileInvalidError as exc:
        raise SheetsCredentialsError(str(exc)) from exc

    try:
        credentials = service_account.Credentials.from_service_account_info(payload, scopes=SCOPES)
    except Exception as exc:  # pragma: no cover - google library guard
        raise SheetsCredentialsError(str(exc)) from exc

    try:
        return build("sheets", "v4", credentials=credentials, cache_discovery=False)
    except Exception as exc:  # pragma: no cover - HTTP / auth error guard
        raise SheetsApiResponseError(str(exc)) from exc


class GoogleSheetsClient:
    """Concrete helper that speaks to Google Sheets using the REST API."""

    def __init__(self, spreadsheet_id: str, credential_path: Path, *, service=None) -> None:
        self._spreadsheet_id = spreadsheet_id
        self._credential_path = credential_path
        self._service = service or _build_service(credential_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def health_check(self) -> None:
        """Perform a lightweight check to confirm the spreadsheet is reachable."""

        try:
            self._service.spreadsheets().get(
                spreadsheetId=self._spreadsheet_id,
                includeGridData=False,
                ranges=[],
            ).execute()
        except HttpError as exc:
            raise SheetsApiResponseError(str(exc)) from exc

    def fetch_tabs(self, titles: Sequence[str], *, columns: int = 52) -> Dict[str, SheetTabData]:
        """Return the rows for each worksheet in ``titles``.

        The request is performed as a single ``batchGet`` call to minimise round
        trips.  Empty worksheets are returned with an empty ``rows`` list.
        """

        if not titles:
            return {}

        ranges = [a1_full_column_range(title, columns=columns) for title in titles]
        try:
            response = (
                self._service.spreadsheets()
                .values()
                .batchGet(spreadsheetId=self._spreadsheet_id, ranges=ranges, majorDimension="ROWS")
                .execute()
            )
        except HttpError as exc:
            raise SheetsApiResponseError(str(exc)) from exc

        value_ranges: Sequence[Mapping[str, object]] = response.get("valueRanges", [])  # type: ignore[assignment]
        results: Dict[str, SheetTabData] = {}

        for title, range_payload in zip(titles, value_ranges):
            values: List[List[str]] = [
                [str(cell) for cell in row]
                for row in range_payload.get("values", [])  # type: ignore[arg-type]
            ]
            if values:
                headers, rows = values[0], values[1:]
            else:
                headers, rows = [], []
            results[title] = SheetTabData(title=title, headers=headers, rows=rows)
        return results

    def update_tabs(
        self,
        payload: Mapping[str, SheetTabData],
        *,
        value_input_option: str = "USER_ENTERED",
    ) -> None:
        """Write the full contents of the provided worksheets in one request."""

        if not payload:
            return

        data: List[Mapping[str, object]] = []
        for tab in payload.values():
            all_rows: List[List[str]] = [list(tab.headers)]
            all_rows.extend([list(row) for row in tab.rows])
            column_count = max([len(tab.headers)] + [len(row) for row in tab.rows]) if tab.rows else len(tab.headers)
            column_count = column_count or 1
            data.append(
                {
                    "range": a1_full_column_range(tab.title, columns=column_count),
                    "values": all_rows,
                    "majorDimension": "ROWS",
                }
            )

        body = {
            "valueInputOption": value_input_option,
            "data": data,
        }

        try:
            (
                self._service.spreadsheets()
                .values()
                .batchUpdate(spreadsheetId=self._spreadsheet_id, body=body)
                .execute()
            )
        except HttpError as exc:
            raise SheetsApiResponseError(str(exc)) from exc


def _normalise_path(candidate: str) -> Path:
    path = Path(candidate).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def is_excel_target(spreadsheet_id: str) -> bool:
    suffix = Path(spreadsheet_id).suffix.lower()
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return True
    if Path(spreadsheet_id).exists():
        return True
    return False


def build_client(spreadsheet_id: str, credential_path: Path) -> GoogleSheetsClient:
    """Factory helper used by higher level modules to construct a client."""

    if is_excel_target(spreadsheet_id):
        path = _normalise_path(spreadsheet_id)
        service = ExcelService(path)
        return GoogleSheetsClient(spreadsheet_id=str(path), credential_path=credential_path, service=service)

    return GoogleSheetsClient(spreadsheet_id=spreadsheet_id, credential_path=credential_path)


__all__ = [
    "GoogleSheetsClient",
    "SheetTabData",
    "SheetsApiResponseError",
    "SheetsClientError",
    "SheetsCredentialsError",
    "SheetsDependencyError",
    "build_client",
    "is_excel_target",
    "GOOGLE_API_AVAILABLE",
    "a1_full_column_range",
    "a1_headers_range",
    "a1_row_range",
]

