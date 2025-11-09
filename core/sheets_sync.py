"""Helpers for interacting with Google Sheets for RugBase synchronisation."""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional

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


SCOPES: Iterable[str] = ("https://www.googleapis.com/auth/spreadsheets",)
DEFAULT_WORKSHEET_TITLE = "items"
METADATA_SHEET = "__metadata__"
METADATA_HEADERS = ["name", "mtime", "sha256"]
METADATA_ROW_KEY = "rugbase.db"
HEADERS: List[str] = [
    "id",
    "rug_no",
    "sku",
    "collection",
    "size",
    "price",
    "qty",
    "updated_at",
    "version",
]
_SPREADSHEET_ID_PATTERN = re.compile(r"/spreadsheets/d/([A-Za-z0-9-_]+)")


class SheetsSyncError(Exception):
    """Base exception for Google Sheets synchronisation errors."""


class MissingDependencyError(SheetsSyncError):
    """Raised when required Google client libraries are not available."""


class CredentialsFileNotFoundError(SheetsSyncError):
    """Raised when the configured credentials file cannot be located."""


class SpreadsheetAccessError(SheetsSyncError):
    """Raised when the Google Sheets API returns an error."""


def is_api_available() -> bool:
    """Return ``True`` if the Google Sheets client libraries are installed."""

    return GOOGLE_API_AVAILABLE


def parse_spreadsheet_id(value: str) -> str:
    """Extract the spreadsheet identifier from a URL or raw ID."""

    if not value:
        return ""

    value = value.strip()
    match = _SPREADSHEET_ID_PATTERN.search(value)
    if match:
        return match.group(1)

    value = value.split("#", 1)[0]
    if "/" in value:
        parts = value.split("/")
        for index, part in enumerate(parts):
            if part == "d" and index + 1 < len(parts):
                return parts[index + 1]
    return value


def _require_api() -> None:
    if not GOOGLE_API_AVAILABLE:
        raise MissingDependencyError(
            "google-api-python-client bulunamadı. Lütfen paketi yükleyin."
        )


def get_client(credentials_path: str):
    """Return an authenticated Sheets API client using the given credentials."""

    _require_api()
    path = Path(os.path.expanduser(credentials_path)).resolve()
    if not path.exists():
        raise CredentialsFileNotFoundError(f"Kimlik dosyası bulunamadı: {path}")

    credentials = service_account.Credentials.from_service_account_file(  # type: ignore[union-attr]
        str(path), scopes=SCOPES
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)  # type: ignore[call-arg]


def _normalise_worksheet(title: Optional[str]) -> str:
    title = (title or DEFAULT_WORKSHEET_TITLE).strip()
    return title or DEFAULT_WORKSHEET_TITLE


def ensure_sheet(service, spreadsheet_id: str, worksheet_title: Optional[str] = None) -> None:
    """Ensure the target spreadsheet has the expected worksheet and headers."""

    spreadsheet_id = parse_spreadsheet_id(spreadsheet_id)
    if not spreadsheet_id:
        raise SpreadsheetAccessError("Geçerli bir Spreadsheet ID gerekli.")

    worksheet = _normalise_worksheet(worksheet_title)

    try:
        metadata = (
            service.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, includeGridData=False)
            .execute()
        )
    except HttpError as exc:  # pragma: no cover - depends on external API
        raise SpreadsheetAccessError(f"Sheets API erişim hatası: {exc}") from exc

    sheets = metadata.get("sheets", []) if isinstance(metadata, dict) else []
    sheet_exists = any(
        worksheet == sheet.get("properties", {}).get("title") for sheet in sheets
    )

    if not sheet_exists:
        logger.info("Creating worksheet '%s'", worksheet)
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {"addSheet": {"properties": {"title": worksheet}}}
                    ]
                },
            ).execute()
        except HttpError as exc:  # pragma: no cover - depends on external API
            raise SpreadsheetAccessError(
                f"Worksheet oluşturulamadı: {exc}"
            ) from exc

    # Ensure header row is present and correct
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=f"{worksheet}!1:1")
            .execute()
        )
    except HttpError as exc:  # pragma: no cover - depends on external API
        raise SpreadsheetAccessError(f"Başlık okunamadı: {exc}") from exc

    header_values = result.get("values", []) if isinstance(result, dict) else []
    if not header_values or [cell.strip() for cell in header_values[0]] != HEADERS:
        logger.info("Writing header row for worksheet '%s'", worksheet)
        try:
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "valueInputOption": "RAW",
                    "data": [
                        {
                            "range": f"{worksheet}!A1",
                            "values": [HEADERS],
                        }
                    ],
                },
            ).execute()
        except HttpError as exc:  # pragma: no cover - depends on external API
            raise SpreadsheetAccessError(
                f"Başlık satırı güncellenemedi: {exc}"
            ) from exc

    _ensure_metadata_sheet(service, spreadsheet_id)


def _ensure_metadata_sheet(service, spreadsheet_id: str) -> None:
    spreadsheet_id = parse_spreadsheet_id(spreadsheet_id)
    if not spreadsheet_id:
        return

    try:
        metadata = (
            service.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, includeGridData=False)
            .execute()
        )
    except HttpError:  # pragma: no cover - defensive fallback
        return

    sheets = metadata.get("sheets", []) if isinstance(metadata, dict) else []
    exists = any(
        METADATA_SHEET == sheet.get("properties", {}).get("title") for sheet in sheets
    )
    if not exists:
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {"addSheet": {"properties": {"title": METADATA_SHEET}}}
                    ]
                },
            ).execute()
        except HttpError:  # pragma: no cover - depends on external API
            return

    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=f"{METADATA_SHEET}!1:1")
            .execute()
        )
    except HttpError:  # pragma: no cover - depends on external API
        return

    header_values = result.get("values", []) if isinstance(result, dict) else []
    if not header_values or [cell.strip() for cell in header_values[0]] != METADATA_HEADERS:
        try:
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "valueInputOption": "RAW",
                    "data": [
                        {
                            "range": f"{METADATA_SHEET}!A1",
                            "values": [METADATA_HEADERS],
                        }
                    ],
                },
            ).execute()
        except HttpError:  # pragma: no cover - depends on external API
            return


def read_database_metadata(service, spreadsheet_id: str) -> Dict[str, str]:
    """Return stored database metadata from the spreadsheet."""

    spreadsheet_id = parse_spreadsheet_id(spreadsheet_id)
    if not spreadsheet_id:
        return {}

    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=f"{METADATA_SHEET}!A2:C")
            .execute()
        )
    except HttpError as exc:  # pragma: no cover - depends on external API
        raise SpreadsheetAccessError(f"Metadata okunamadı: {exc}") from exc

    values = result.get("values", []) if isinstance(result, dict) else []
    for row in values:
        padded = list(row) + [""] * max(0, len(METADATA_HEADERS) - len(row))
        if padded and padded[0].strip() == METADATA_ROW_KEY:
            return {
                "name": METADATA_ROW_KEY,
                "mtime": padded[1].strip(),
                "sha256": padded[2].strip(),
            }
    return {}


def write_database_metadata(service, spreadsheet_id: str, mtime: str, sha256: str) -> None:
    """Persist local database metadata to the spreadsheet."""

    spreadsheet_id = parse_spreadsheet_id(spreadsheet_id)
    if not spreadsheet_id:
        raise SpreadsheetAccessError("Geçerli bir Spreadsheet ID gerekli.")

    _ensure_metadata_sheet(service, spreadsheet_id)
    payload = [
        {
            "range": f"{METADATA_SHEET}!A2",
            "values": [[METADATA_ROW_KEY, mtime, sha256]],
        }
    ]

    try:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": payload},
        ).execute()
    except HttpError as exc:  # pragma: no cover - depends on external API
        raise SpreadsheetAccessError(f"Metadata yazılamadı: {exc}") from exc


def verify_roundtrip(service, spreadsheet_id: str, worksheet_title: Optional[str] = None) -> None:
    """Write a sentinel value and clear it to verify API access."""

    spreadsheet_id = parse_spreadsheet_id(spreadsheet_id)
    if not spreadsheet_id:
        raise SpreadsheetAccessError("Geçerli bir Spreadsheet ID gerekli.")

    worksheet = _normalise_worksheet(worksheet_title)
    test_range = f"{worksheet}!A1"

    try:
        existing = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=test_range)
            .execute()
        )
    except HttpError as exc:  # pragma: no cover - depends on external API
        raise SpreadsheetAccessError(f"Bağlantı doğrulaması başarısız: {exc}") from exc

    original = existing.get("values", []) if isinstance(existing, dict) else []
    if not original:
        original = [[""]]

    try:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "RAW",
                "data": [
                    {
                        "range": test_range,
                        "values": [["RugBase OK"]],
                    }
                ],
            },
        ).execute()
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "RAW",
                "data": [
                    {
                        "range": test_range,
                        "values": original,
                    }
                ],
            },
        ).execute()
    except HttpError as exc:  # pragma: no cover - depends on external API
        raise SpreadsheetAccessError(f"Bağlantı doğrulaması başarısız: {exc}") from exc


def read_rows(service, spreadsheet_id: str, worksheet_title: Optional[str] = None) -> List[Dict[str, str]]:
    """Return all data rows from the worksheet as dictionaries."""

    spreadsheet_id = parse_spreadsheet_id(spreadsheet_id)
    if not spreadsheet_id:
        return []

    worksheet = _normalise_worksheet(worksheet_title)

    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=f"{worksheet}!A2:I")
            .execute()
        )
    except HttpError as exc:  # pragma: no cover - depends on external API
        raise SpreadsheetAccessError(f"Satırlar okunamadı: {exc}") from exc

    values = result.get("values", []) if isinstance(result, dict) else []
    rows: List[Dict[str, str]] = []
    for raw in values:
        padded = list(raw) + [""] * max(0, len(HEADERS) - len(raw))
        rows.append({key: padded[index] for index, key in enumerate(HEADERS)})
    return rows


def write_rows(
    service, spreadsheet_id: str, rows: List[Dict[str, str]], worksheet_title: Optional[str] = None
) -> None:
    """Replace all worksheet data rows with ``rows``."""

    spreadsheet_id = parse_spreadsheet_id(spreadsheet_id)
    if not spreadsheet_id:
        raise SpreadsheetAccessError("Geçerli bir Spreadsheet ID gerekli.")

    worksheet = _normalise_worksheet(worksheet_title)
    values = [[row.get(header, "") for header in HEADERS] for row in rows]
    data = [
        {
            "range": f"{worksheet}!A1",
            "values": [HEADERS],
        }
    ]
    if values:
        data.append({"range": f"{worksheet}!A2", "values": values})

    try:
        service.spreadsheets().values().batchClear(
            spreadsheetId=spreadsheet_id, body={"ranges": [f"{worksheet}!A2:Z"]}
        ).execute()
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()
    except HttpError as exc:  # pragma: no cover - depends on external API
        raise SpreadsheetAccessError(f"Satırlar yazılamadı: {exc}") from exc


def upsert_rows(
    service,
    spreadsheet_id: str,
    rows: List[Dict[str, str]],
    worksheet_title: Optional[str] = None,
) -> List[Dict[str, str]]:
    """Merge ``rows`` into the worksheet and return the resulting dataset."""

    if not rows:
        return read_rows(service, spreadsheet_id, worksheet_title)

    existing = read_rows(service, spreadsheet_id, worksheet_title)
    replacements = {
        row.get("id"): row for row in rows if row.get("id")
    }
    updated: List[Dict[str, str]] = []
    seen = set()

    for current in existing:
        row_id = current.get("id")
        if row_id and row_id in replacements:
            updated.append(replacements[row_id])
            seen.add(row_id)
        else:
            updated.append(current)

    for row in rows:
        row_id = row.get("id")
        if not row_id or row_id in seen:
            continue
        updated.append(row)

    write_rows(service, spreadsheet_id, updated, worksheet_title)
    return updated


__all__ = [
    "HEADERS",
    "DEFAULT_WORKSHEET_TITLE",
    "SheetsSyncError",
    "MissingDependencyError",
    "CredentialsFileNotFoundError",
    "SpreadsheetAccessError",
    "GOOGLE_API_AVAILABLE",
    "get_client",
    "ensure_sheet",
    "read_rows",
    "write_rows",
    "upsert_rows",
    "read_database_metadata",
    "write_database_metadata",
    "verify_roundtrip",
    "parse_spreadsheet_id",
    "is_api_available",
]
