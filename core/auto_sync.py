"""Background controller that keeps the local database in sync automatically."""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Optional

import db
from core.hash import file_sha256
from core import sheets_sync
from core.conflicts import recent as recent_conflicts
from core.offline_queue import OutboxQueue
from settings import GoogleSyncSettings, load_google_sync_settings

logger = logging.getLogger(__name__)


StatusPayload = Dict[str, object]
StatusCallback = Callable[[str, StatusPayload], None]
LogCallback = Callable[[str], None]
ConflictCallback = Callable[[StatusPayload], None]


@dataclass
class AutoSyncState:
    last_local_hash: Optional[str] = None
    last_local_updated_at: Optional[str] = None
    last_remote_updated_at: Optional[str] = None


class AutoSyncController:
    """Manage scheduled push/pull operations on a daemon thread."""

    def __init__(
        self,
        *,
        settings_provider: Optional[Callable[[], GoogleSyncSettings]] = None,
        interval_seconds: int = 60,
        status_callback: Optional[StatusCallback] = None,
        log_callback: Optional[LogCallback] = None,
        conflict_callback: Optional[ConflictCallback] = None,
    ) -> None:
        self._settings_provider = settings_provider or load_google_sync_settings
        self._interval = max(10, interval_seconds)
        self._status_callback = status_callback
        self._log_callback = log_callback
        self._conflict_callback = conflict_callback
        self._state = AutoSyncState()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._outbox = OutboxQueue()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
            self._thread = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _run(self) -> None:
        while not self._stop_event.is_set():
            start = time.monotonic()
            try:
                self._tick()
            except Exception:  # pragma: no cover - defensive guard
                logger.exception("Auto sync tick failed")
                self._notify_status("error", {"message": "unexpected failure"})
            elapsed = time.monotonic() - start
            remaining = max(0.0, self._interval - elapsed)
            waited = 0.0
            while waited < remaining and not self._stop_event.is_set():
                time.sleep(min(1.0, remaining - waited))
                waited += 1.0

    def _tick(self) -> None:
        if not sheets_sync.is_api_available():
            self._notify_status("disabled", {"reason": "dependencies"})
            return

        settings = self._settings_provider()
        if not settings or not settings.spreadsheet_id:
            self._notify_status("disabled", {"reason": "settings"})
            return

        try:
            sheets_sync.resolve_credentials_path(settings.credential_path)
        except sheets_sync.CredentialsFileNotFoundError:
            self._notify_status("disabled", {"reason": "credentials"})
            return

        try:
            worksheet_title = sheets_sync.require_worksheet_title(settings.worksheet_title)
        except sheets_sync.SpreadsheetAccessError as exc:
            self._notify_status("disabled", {"reason": "worksheet", "message": str(exc)})
            return

        local_updated_at = db.get_max_item_updated_at()
        local_hash = None
        db_path = Path(db.DB_PATH)
        if db_path.exists():
            try:
                local_hash = file_sha256(str(db_path))
            except OSError:  # pragma: no cover - filesystem guard
                local_hash = None

        client = sheets_sync.get_client(settings.credential_path)
        parsed_id = sheets_sync.parse_spreadsheet_id(settings.spreadsheet_id)
        if not parsed_id:
            self._notify_status("error", {"message": "invalid spreadsheet id"})
            return

        try:
            remote_updated_at = sheets_sync.latest_remote_updated_at(
                client, parsed_id, worksheet_title
            )
        except sheets_sync.SheetsSyncError as exc:
            self._notify_status("offline", {"message": str(exc)})
            return

        has_local_changes = (
            local_updated_at != self._state.last_local_updated_at
            or local_hash != self._state.last_local_hash
        )
        has_remote_changes = (
            remote_updated_at is not None
            and remote_updated_at != self._state.last_remote_updated_at
        )
        has_outbox = self._outbox.path.exists()

        if not (has_local_changes or has_remote_changes or has_outbox):
            self._notify_status("idle", {})
            return

        try:
            pull_stats = sheets_sync.pull(
                settings.spreadsheet_id,
                settings.credential_path,
                worksheet_title=worksheet_title,
                db_path=db.DB_PATH,
                log_callback=self._log,
            )
            push_stats = sheets_sync.push(
                settings.spreadsheet_id,
                settings.credential_path,
                worksheet_title=worksheet_title,
                db_path=db.DB_PATH,
                log_callback=self._log,
            )
        except sheets_sync.SheetsSyncError as exc:
            self._notify_status("offline", {"message": str(exc)})
            return

        # Refresh state after successful sync
        self._state.last_local_updated_at = db.get_max_item_updated_at()
        if db_path.exists():
            try:
                self._state.last_local_hash = file_sha256(str(db_path))
            except OSError:  # pragma: no cover - filesystem guard
                self._state.last_local_hash = None
        else:
            self._state.last_local_hash = None
        self._state.last_remote_updated_at = remote_updated_at

        payload: StatusPayload = {
            "pull": pull_stats,
            "push": push_stats,
        }
        self._notify_status("synced", payload)
        self._emit_conflicts()

    def _log(self, message: str) -> None:
        if self._log_callback:
            try:
                self._log_callback(message)
            except Exception:  # pragma: no cover - UI callback guard
                logger.debug("Auto sync log callback failed", exc_info=True)

    def _notify_status(self, status: str, payload: StatusPayload) -> None:
        if self._status_callback:
            try:
                self._status_callback(status, payload)
            except Exception:  # pragma: no cover - UI callback guard
                logger.debug("Auto sync status callback failed", exc_info=True)

    def _emit_conflicts(self) -> None:
        if not self._conflict_callback:
            return
        try:
            items = recent_conflicts(limit=10)
            self._conflict_callback({"items": items})
        except Exception:  # pragma: no cover - UI callback guard
            logger.debug("Conflict callback failed", exc_info=True)


__all__ = ["AutoSyncController"]
