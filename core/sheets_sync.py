"""Helpers for interacting with Google Sheets for RugBase synchronisation."""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List

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
SHEET_NAME = "items"
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


def ensure_sheet(service, spreadsheet_id: str) -> None:
    """Ensure the target spreadsheet has the expected worksheet and headers."""

    spreadsheet_id = parse_spreadsheet_id(spreadsheet_id)
    if not spreadsheet_id:
        raise SpreadsheetAccessError("Geçerli bir Spreadsheet ID gerekli.")

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
        SHEET_NAME == sheet.get("properties", {}).get("title") for sheet in sheets
    )

    if not sheet_exists:
        logger.info("Creating worksheet '%s'", SHEET_NAME)
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {"addSheet": {"properties": {"title": SHEET_NAME}}}
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
            .get(spreadsheetId=spreadsheet_id, range=f"{SHEET_NAME}!1:1")
            .execute()
        )
    except HttpError as exc:  # pragma: no cover - depends on external API
        raise SpreadsheetAccessError(f"Başlık okunamadı: {exc}") from exc

    header_values = result.get("values", []) if isinstance(result, dict) else []
    if not header_values or [cell.strip() for cell in header_values[0]] != HEADERS:
        logger.info("Writing header row for worksheet '%s'", SHEET_NAME)
        try:
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{SHEET_NAME}!A1",
                valueInputOption="RAW",
                body={"values": [HEADERS]},
            ).execute()
        except HttpError as exc:  # pragma: no cover - depends on external API
            raise SpreadsheetAccessError(
                f"Başlık satırı güncellenemedi: {exc}"
            ) from exc


def read_rows(service, spreadsheet_id: str) -> List[Dict[str, str]]:
    """Return all data rows from the worksheet as dictionaries."""

    spreadsheet_id = parse_spreadsheet_id(spreadsheet_id)
    if not spreadsheet_id:
        return []

    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=f"{SHEET_NAME}!A2:I")
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


def write_rows(service, spreadsheet_id: str, rows: List[Dict[str, str]]) -> None:
    """Replace all worksheet data rows with ``rows``."""

    spreadsheet_id = parse_spreadsheet_id(spreadsheet_id)
    if not spreadsheet_id:
        raise SpreadsheetAccessError("Geçerli bir Spreadsheet ID gerekli.")

    values = [[row.get(header, "") for header in HEADERS] for row in rows]
    body = {"values": [HEADERS, *values]}

    try:
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=f"{SHEET_NAME}!A2:Z"
        ).execute()
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{SHEET_NAME}!A1",
            valueInputOption="RAW",
            body=body,
        ).execute()
    except HttpError as exc:  # pragma: no cover - depends on external API
        raise SpreadsheetAccessError(f"Satırlar yazılamadı: {exc}") from exc


def upsert_rows(
    service, spreadsheet_id: str, rows: List[Dict[str, str]]
) -> List[Dict[str, str]]:
    """Merge ``rows`` into the worksheet and return the resulting dataset."""

    if not rows:
        return read_rows(service, spreadsheet_id)

    existing = read_rows(service, spreadsheet_id)
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

    write_rows(service, spreadsheet_id, updated)
    return updated


__all__ = [
    "HEADERS",
    "SHEET_NAME",
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
    "parse_spreadsheet_id",
    "is_api_available",
]
