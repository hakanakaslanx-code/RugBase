"""Background synchronisation worker for RugBase."""
from __future__ import annotations

import threading
from typing import Callable, Optional

from core import sync


StatusCallback = Callable[[str, Optional[sync.SyncResult]], None]


class SyncWorker:
    """Run pull/apply cycles on a background thread and report status to the UI."""

    def __init__(self, root, status_callback: Optional[StatusCallback] = None) -> None:
        self.root = root
        self.status_callback = status_callback
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

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

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._execute_sync()
            interval = sync.get_poll_interval()
            if interval <= 0:
                interval = sync.DEFAULT_POLL_INTERVAL
            if self._stop_event.wait(interval):
                break

    def _execute_sync(self) -> None:
        if not self._lock.acquire(blocking=False):
            return
        try:
            try:
                result = sync.pull_and_apply()
                message = self._format_sync_message(result)
            except sync.SyncConfigurationError:
                result = None
                message = "Drive sync is not configured."
            except Exception as exc:  # pragma: no cover - best effort logging
                result = None
                message = f"Sync failed: {exc}"
        finally:
            self._lock.release()
        self._dispatch_status(message, result)

    def _execute_backup(self) -> None:
        if not self._lock.acquire(blocking=False):
            return
        try:
            try:
                archive_name = sync.backup_now()
                message = f"Backup uploaded: {archive_name}"
            except sync.SyncConfigurationError:
                message = "Drive sync is not configured."
            except Exception as exc:  # pragma: no cover - best effort logging
                message = f"Backup failed: {exc}"
        finally:
            self._lock.release()
        self._dispatch_status(message, None)

    def _dispatch_status(self, message: str, result: Optional[sync.SyncResult]) -> None:
        if not self.status_callback:
            return

        def callback() -> None:
            self.status_callback(message, result)

        self.root.after(0, callback)

    @staticmethod
    def _format_sync_message(result: sync.SyncResult) -> str:
        timestamp = result.last_sync or "n/a"
        conflict_part = (
            f"; {result.total_conflicts} conflict(s)" if result.total_conflicts else ""
        )
        return (
            f"Last sync: {timestamp} ({result.applied} change(s) applied, "
            f"{result.processed} file(s) processed{conflict_part})"
        )
