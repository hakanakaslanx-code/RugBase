"""Business logic for synchronising the local database with Google Sheets."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import db

from . import sheets_sync
from .hash import file_sha256
from settings import GoogleSyncSettings

logger = logging.getLogger(__name__)

ConflictDecision = str
ConflictResolver = Callable[[Dict[str, Any], Dict[str, Any]], ConflictDecision]

_VALID_DECISIONS = {"local", "remote", "skip"}


class SyncServiceError(Exception):
    """Raised when the synchronisation service cannot complete an operation."""


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    candidate = value
    if value.endswith("Z"):
        candidate = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed


def _normalise_remote_row(row: Dict[str, str]) -> Dict[str, Any]:
    def _clean(key: str) -> str:
        return (row.get(key) or "").strip()

    def _to_int(key: str, default: int = 0) -> int:
        raw = _clean(key)
        if not raw:
            return default
        try:
            return int(float(raw))
        except ValueError:
            return default

    return {
        "id": _clean("id"),
        "rug_no": _clean("rug_no"),
        "sku": _clean("sku"),
        "collection": _clean("collection"),
        "size": _clean("size"),
        "price": _clean("price"),
        "qty": _to_int("qty"),
        "updated_at": _clean("updated_at"),
        "version": max(_to_int("version", default=1), 1),
    }


def _rows_equal(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    for key in ("rug_no", "sku", "collection", "size", "price", "qty"):
        left_value = (left.get(key) or "").strip() if isinstance(left.get(key), str) else left.get(key)
        right_value = (right.get(key) or "").strip() if isinstance(right.get(key), str) else right.get(key)
        if str(left_value) != str(right_value):
            return False
    return True


def _local_database_path() -> Path:
    return Path(db.DB_PATH).resolve()


def _local_metadata() -> Dict[str, str]:
    path = _local_database_path()
    if not path.exists():
        return {}
    try:
        stat = path.stat()
    except OSError:
        return {}
    mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
    return {
        "name": sheets_sync.METADATA_ROW_KEY,
        "mtime": mtime,
        "sha256": file_sha256(str(path)),
    }


def _prepare_sheet_row(row: Dict[str, Any]) -> Dict[str, str]:
    prepared: Dict[str, str] = {}
    for key in sheets_sync.HEADERS:
        value = row.get(key, "")
        if value is None:
            prepared[key] = ""
        elif key in {"qty", "version"}:
            try:
                prepared[key] = str(int(value))
            except (TypeError, ValueError):
                prepared[key] = "0"
        else:
            prepared[key] = str(value)
    return prepared


class SyncService:
    """Coordinate pull, push, and auto-sync operations."""

    def __init__(
        self,
        log_callback: Optional[Callable[[str], None]] = None,
        conflict_resolver: Optional[ConflictResolver] = None,
    ) -> None:
        self._log_callback = log_callback
        self._conflict_resolver = conflict_resolver

    # ------------------------------------------------------------------
    # Public operations
    # ------------------------------------------------------------------
    def test_connection(self, settings: GoogleSyncSettings) -> Dict[str, str]:
        """Ensure that credentials and spreadsheet access are valid."""

        report: Dict[str, str] = {}

        if not sheets_sync.is_api_available():
            raise sheets_sync.MissingDependencyError(
                "google-api-python-client was not found. Verify the pydeps folder via"
                " Tools → Developer Log."
            )

        missing_imports = []
        for dotted_path in (
            "googleapiclient.discovery",
            "googleapiclient.http",
            "google.oauth2.service_account",
            "google_auth_oauthlib.flow",
        ):
            try:
                __import__(dotted_path)
            except ImportError as exc:
                missing_imports.append(f"{dotted_path}: {exc}")

        if missing_imports:
            raise sheets_sync.MissingDependencyError(
                "Google API import test failed: " + "; ".join(missing_imports)
            )

        report["imports"] = "Google API libraries loaded successfully."

        client = sheets_sync.get_client(settings.credential_path)
        worksheet_title = sheets_sync.require_worksheet_title(settings.worksheet_title)
        sheets_sync.ensure_sheet(client, settings.spreadsheet_id, worksheet_title)

        escaped_title = worksheet_title.replace("'", "''")
        range_spec = f"'{escaped_title}'!{sheets_sync.FULL_COLUMN_RANGE}"
        try:
            result = (
                client.spreadsheets()
                .values()
                .get(
                    spreadsheetId=sheets_sync.parse_spreadsheet_id(settings.spreadsheet_id),
                    range=range_spec,
                )
                .execute()
            )
        except sheets_sync.HttpError as exc:  # type: ignore[attr-defined]
            raise sheets_sync.SpreadsheetAccessError(
                f"Sheets 'values.get' test failed: {exc}"
            ) from exc

        values = result.get("values", []) if isinstance(result, dict) else []
        first_value = values[0][0] if values and values[0] else ""
        report["values_get"] = f"Cell A1 read: '{first_value}'."

        sheets_sync.verify_roundtrip(client, settings.spreadsheet_id, worksheet_title)
        report["roundtrip"] = "Sheets write/read verification completed."

        return report

    def get_local_metadata(self) -> Dict[str, str]:
        """Return metadata for the local database file."""

        return _local_metadata()

    def get_remote_metadata(self, settings: GoogleSyncSettings) -> Dict[str, str]:
        """Fetch the metadata stored alongside the spreadsheet."""

        client = sheets_sync.get_client(settings.credential_path)
        worksheet_title = sheets_sync.require_worksheet_title(settings.worksheet_title)
        sheets_sync.ensure_sheet(client, settings.spreadsheet_id, worksheet_title)
        return sheets_sync.read_database_metadata(client, settings.spreadsheet_id)

    def pull(self, settings: GoogleSyncSettings) -> Dict[str, int]:
        """Synchronise data from Google Sheets into SQLite."""

        client = sheets_sync.get_client(settings.credential_path)
        worksheet_title = sheets_sync.require_worksheet_title(settings.worksheet_title)
        sheets_sync.ensure_sheet(client, settings.spreadsheet_id, worksheet_title)
        remote_rows = sheets_sync.read_rows(
            client, settings.spreadsheet_id, worksheet_title
        )

        stats = {"inserted": 0, "updated": 0, "skipped": 0, "conflicts": 0}

        for raw_row in remote_rows:
            remote = _normalise_remote_row(raw_row)
            item_id = remote.get("id")
            if not item_id:
                self._log("Sheets row missing identifier; skipped.")
                stats["skipped"] += 1
                continue

            local = db.fetch_item_for_sync(item_id)
            if not local:
                db.apply_remote_sync_row(remote)
                self._log(f"Sheets -> SQLite: new record added ({item_id})")
                stats["inserted"] += 1
                continue

            remote_version = remote.get("version", 1)
            local_version = int(local.get("version") or 1)
            if remote_version > local_version:
                db.apply_remote_sync_row(remote)
                self._log(f"Sheets -> SQLite: updated ({item_id})")
                stats["updated"] += 1
                continue

            if remote_version < local_version:
                stats["skipped"] += 1
                continue

            remote_time = _parse_timestamp(remote.get("updated_at"))
            local_time = _parse_timestamp(local.get("updated_at"))
            if remote_time and local_time:
                if remote_time > local_time:
                    db.apply_remote_sync_row(remote)
                    self._log(f"Sheets -> SQLite: updated ({item_id})")
                    stats["updated"] += 1
                    continue
                if remote_time < local_time:
                    stats["skipped"] += 1
                    continue

            if _rows_equal(local, remote):
                stats["skipped"] += 1
                continue

            decision = self._resolve_conflict(local, remote)
            if decision == "remote":
                db.apply_remote_sync_row(remote)
                self._log(f"Conflict resolution: Sheets data accepted ({item_id})")
                stats["updated"] += 1
            elif decision == "local":
                db.bump_item_version(item_id)
                self._log(f"Conflict resolution: local data retained ({item_id})")
                stats["skipped"] += 1
            else:
                self._log(f"Conflict resolution: action skipped ({item_id})")
                stats["conflicts"] += 1

        self._persist_metadata(client, settings)
        return stats

    def push(self, settings: GoogleSyncSettings) -> Dict[str, int]:
        """Synchronise data from SQLite to Google Sheets."""

        client = sheets_sync.get_client(settings.credential_path)
        worksheet_title = sheets_sync.require_worksheet_title(settings.worksheet_title)
        sheets_sync.ensure_sheet(client, settings.spreadsheet_id, worksheet_title)
        remote_rows = sheets_sync.read_rows(
            client, settings.spreadsheet_id, worksheet_title
        )
        remote_index = {
            row.get("id"): _normalise_remote_row(row) for row in remote_rows if row.get("id")
        }

        stats = {"inserted": 0, "updated": 0, "skipped": 0, "conflicts": 0}
        pending_updates: List[Dict[str, str]] = []

        for local in db.fetch_items_for_sync_snapshot():
            item_id = local.get("id")
            if not item_id:
                continue
            remote = remote_index.get(item_id)
            local_version = int(local.get("version") or 1)

            if not remote:
                pending_updates.append(_prepare_sheet_row(local))
                self._log(f"SQLite -> Sheets: new record queued ({item_id})")
                stats["inserted"] += 1
                continue

            remote_version = remote.get("version", 1)
            if local_version > remote_version:
                pending_updates.append(_prepare_sheet_row(local))
                self._log(f"SQLite -> Sheets: update scheduled ({item_id})")
                stats["updated"] += 1
                continue
            if local_version < remote_version:
                stats["skipped"] += 1
                continue

            remote_time = _parse_timestamp(remote.get("updated_at"))
            local_time = _parse_timestamp(local.get("updated_at"))
            if remote_time and local_time:
                if local_time > remote_time:
                    pending_updates.append(_prepare_sheet_row(local))
                    self._log(f"SQLite -> Sheets: update scheduled ({item_id})")
                    stats["updated"] += 1
                    continue
                if local_time < remote_time:
                    stats["skipped"] += 1
                    continue

            if _rows_equal(local, remote):
                stats["skipped"] += 1
                continue

            decision = self._resolve_conflict(local, remote)
            if decision == "local":
                pending_updates.append(_prepare_sheet_row(local))
                self._log(f"Conflict resolution: local data will be sent to Sheets ({item_id})")
                stats["updated"] += 1
            elif decision == "remote":
                db.apply_remote_sync_row(remote)
                self._log(f"Conflict resolution: Sheets data retained ({item_id})")
                stats["skipped"] += 1
            else:
                self._log(f"Conflict resolution: action skipped ({item_id})")
                stats["conflicts"] += 1

        if pending_updates:
            sheets_sync.upsert_rows(
                client,
                settings.spreadsheet_id,
                pending_updates,
                worksheet_title,
            )
        self._persist_metadata(client, settings)

        return stats

    def autosync(self, settings: GoogleSyncSettings) -> Dict[str, Dict[str, int]]:
        """Perform a pull followed by a push."""

        pull_stats = self.pull(settings)
        push_stats = self.push(settings)
        return {"pull": pull_stats, "push": push_stats}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _persist_metadata(self, client, settings: GoogleSyncSettings) -> None:
        metadata = _local_metadata()
        if not metadata:
            return
        try:
            sheets_sync.write_database_metadata(
                client,
                settings.spreadsheet_id,
                metadata.get("mtime", ""),
                metadata.get("sha256", ""),
            )
        except sheets_sync.SheetsSyncError as exc:
            self._log(f"Metadata could not be updated: {exc}")

    def _log(self, message: str) -> None:
        if self._log_callback:
            try:
                self._log_callback(message)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Sync log callback failed")

    def _resolve_conflict(
        self, local: Dict[str, Any], remote: Dict[str, Any]
    ) -> ConflictDecision:
        if not self._conflict_resolver:
            return "local"
        try:
            decision = self._conflict_resolver(local, remote)
        except Exception as exc:  # pragma: no cover - UI callback failure
            logger.exception("Conflict resolver callback failed: %s", exc)
            return "skip"
        if decision not in _VALID_DECISIONS:
            return "skip"
        return decision


__all__ = [
    "ConflictDecision",
    "ConflictResolver",
    "SyncService",
    "SyncServiceError",
]
