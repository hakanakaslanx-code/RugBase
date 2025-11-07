"""Background synchronisation worker for RugBase."""
from __future__ import annotations

import logging
import threading
from typing import Callable, Optional

from core import drive_api, drive_sync
from core.drive_sync import (
    DEFAULT_POLL_INTERVAL,
    DriveSync,
    SyncAuthenticationRequired,
    SyncConfigurationError,
    SyncOfflineError,
    SyncResult,
    get_poll_interval,
)


StatusCallback = Callable[[str, Optional[SyncResult]], None]


class SyncWorker:
    """Run pull/apply cycles on a background thread and report status to the UI."""

    def __init__(self, root, status_callback: Optional[StatusCallback] = None) -> None:
        self.root = root
        self.status_callback = status_callback
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._sync = DriveSync()
        self._logger = logging.getLogger(__name__)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None

    def sync_now(self) -> None:
        threading.Thread(target=self._execute_sync, daemon=True).start()

    def backup_now(self) -> None:
        threading.Thread(target=self._execute_backup, daemon=True).start()

    def restore_now(self) -> None:
        threading.Thread(target=self._execute_restore, daemon=True).start()

    def reset_connection(self) -> None:
        threading.Thread(target=self._execute_reset, daemon=True).start()

    def resolve_conflict(self, prefer_local: bool) -> None:
        threading.Thread(
            target=self._execute_conflict_resolution,
            args=(prefer_local,),
            daemon=True,
        ).start()

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._execute_sync()
            interval = get_poll_interval()
            if interval <= 0:
                interval = DEFAULT_POLL_INTERVAL
            if self._stop_event.wait(interval):
                break

    def _execute_sync(self) -> None:
        if not self._lock.acquire(blocking=False):
            return
        try:
            try:
                result = self._sync.sync_once()
            except SyncConfigurationError as exc:
                result = SyncResult(
                    action="error",
                    message=str(exc),
                    status=drive_sync.STATUS_REAUTHORISE,
                )
            except SyncAuthenticationRequired as exc:
                result = SyncResult(
                    action="error",
                    message=str(exc),
                    status=drive_sync.STATUS_REAUTHORISE,
                )
            except SyncOfflineError as exc:
                result = SyncResult(
                    action="error",
                    message=str(exc),
                    status=drive_sync.STATUS_OFFLINE,
                )
            except drive_api.HttpError as exc:  # pragma: no cover - depends on API runtime
                status_code = getattr(getattr(exc, "resp", None), "status", None)
                status = (
                    drive_sync.STATUS_REAUTHORISE
                    if status_code in (401, 403)
                    else drive_sync.STATUS_OFFLINE
                )
                result = SyncResult(
                    action="error",
                    message=f"Drive API error: {exc}",
                    status=status,
                )
                self._logger.warning("Drive API error during sync: %s", exc, exc_info=True)
            except Exception as exc:  # pragma: no cover - best effort logging
                result = SyncResult(
                    action="error",
                    message=f"Sync failed: {exc}",
                    status=drive_sync.STATUS_OFFLINE,
                )
                self._logger.exception("Unexpected error during sync", exc_info=True)
        finally:
            self._lock.release()
        self._dispatch_status(result.message, result)

    def _execute_backup(self) -> None:
        if not self._lock.acquire(blocking=False):
            return
        try:
            try:
                archive_name = self._sync.backup_local()
                result = SyncResult(
                    action="backup",
                    message=f"Backup uploaded: {archive_name}",
                    status=drive_sync.STATUS_CONNECTED,
                )
            except SyncConfigurationError as exc:
                result = SyncResult(
                    action="error",
                    message=str(exc),
                    status=drive_sync.STATUS_REAUTHORISE,
                )
            except SyncAuthenticationRequired as exc:
                result = SyncResult(
                    action="error",
                    message=str(exc),
                    status=drive_sync.STATUS_REAUTHORISE,
                )
            except Exception as exc:  # pragma: no cover - best effort logging
                result = SyncResult(
                    action="error",
                    message=f"Backup failed: {exc}",
                    status=drive_sync.STATUS_OFFLINE,
                )
                self._logger.exception("Unexpected error during backup", exc_info=True)
        finally:
            self._lock.release()
        self._dispatch_status(result.message, result)

    def _execute_restore(self) -> None:
        if not self._lock.acquire(blocking=False):
            return
        try:
            try:
                result = self._sync.restore_remote()
            except SyncConfigurationError as exc:
                result = SyncResult(
                    action="error",
                    message=str(exc),
                    status=drive_sync.STATUS_REAUTHORISE,
                )
            except SyncAuthenticationRequired as exc:
                result = SyncResult(
                    action="error",
                    message=str(exc),
                    status=drive_sync.STATUS_REAUTHORISE,
                )
            except Exception as exc:
                result = SyncResult(
                    action="error",
                    message=f"Restore failed: {exc}",
                    status=drive_sync.STATUS_OFFLINE,
                )
                self._logger.exception("Unexpected error during restore", exc_info=True)
        finally:
            self._lock.release()
        self._dispatch_status(result.message, result)

    def _execute_reset(self) -> None:
        if not self._lock.acquire(blocking=False):
            return
        try:
            try:
                self._sync.reset_credentials()
                result = SyncResult(
                    action="reset",
                    message="Connection reset. Please re-authorise.",
                    status=drive_sync.STATUS_REAUTHORISE,
                )
            except Exception as exc:
                result = SyncResult(
                    action="error",
                    message=f"Failed to reset connection: {exc}",
                    status=drive_sync.STATUS_REAUTHORISE,
                )
                self._logger.exception("Failed to reset sync connection", exc_info=True)
        finally:
            self._lock.release()
        self._dispatch_status(result.message, result)

    def _execute_conflict_resolution(self, prefer_local: bool) -> None:
        if not self._lock.acquire(blocking=False):
            return
        try:
            try:
                result = self._sync.resolve_conflict(prefer_local)
            except SyncConfigurationError as exc:
                result = SyncResult(
                    action="error",
                    message=str(exc),
                    status=drive_sync.STATUS_REAUTHORISE,
                )
            except SyncAuthenticationRequired as exc:
                result = SyncResult(
                    action="error",
                    message=str(exc),
                    status=drive_sync.STATUS_REAUTHORISE,
                )
            except Exception as exc:
                direction = "local" if prefer_local else "remote"
                result = SyncResult(
                    action="error",
                    message=f"Failed to apply {direction} copy: {exc}",
                    status=drive_sync.STATUS_OFFLINE,
                )
                self._logger.exception("Conflict resolution failed", exc_info=True)
        finally:
            self._lock.release()
        self._dispatch_status(result.message, result)

    def _dispatch_status(self, message: str, result: Optional[SyncResult]) -> None:
        if not self.status_callback:
            return

        def callback() -> None:
            self.status_callback(message, result)

        self.root.after(0, callback)
