"""Background synchronisation manager for Google Sheets."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional

import db
from core.sheets_client import SheetsClientError

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return (
        datetime.utcnow()
        .replace(microsecond=0)
        .replace(tzinfo=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


@dataclass
class InventoryStatus:
    """Status payload reported to UI consumers."""

    online: bool
    last_sync: Optional[str] = None
    pending: int = 0
    message: Optional[str] = None
    error: Optional[str] = None
    conflicts: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "online": self.online,
            "pending": self.pending,
        }
        if self.last_sync:
            payload["last_sync"] = self.last_sync
        if self.message:
            payload["message"] = self.message
        if self.error:
            payload["error"] = self.error
        if self.conflicts:
            payload["conflicts"] = list(self.conflicts)
        return payload


StatusCallback = Callable[[InventoryStatus], None]


class InventorySyncManager:
    """Manage periodic synchronisation with Google Sheets."""

    def __init__(
        self,
        *,
        poll_interval: float = 5.0,
        status_callback: Optional[StatusCallback] = None,
    ) -> None:
        self._status_callback = status_callback
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._online = False
        self._last_sync: Optional[str] = None
        self._pending = 0
        self._poll_interval = poll_interval

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        db.add_item_upsert_listener(self._on_item_upsert)
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        db.remove_item_upsert_listener(self._on_item_upsert)
        self._stop_event.set()
        self._wake_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self._thread = None

    def sync_now(self) -> None:
        self._wake_event.set()

    def load_initial_snapshot(self) -> bool:
        try:
            changed = db.refresh_from_remote()
        except SheetsClientError:
            logger.exception("Initial Google Sheets sync failed")
            return False
        self._online = db.is_online()
        if self._online:
            self._last_sync = _utc_now_iso()
            self._notify_status(message="Data updated" if changed else None)
        else:
            self._notify_status(error=db.last_sync_error())
        return self._online

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            triggered = self._wake_event.wait(timeout=self._poll_interval)
            self._wake_event.clear()
            try:
                self._sync_once(force_pull=triggered)
            except SheetsClientError:
                logger.exception("Background sync cycle failed")
                self._set_offline("Synchronization error")

    def _notify_status(
        self,
        *,
        message: Optional[str] = None,
        error: Optional[str] = None,
        conflicts: Optional[Iterable[str]] = None,
    ) -> None:
        status = InventoryStatus(
            online=self._online,
            last_sync=self._last_sync,
            pending=self._pending,
            message=message,
            error=error,
            conflicts=list(conflicts or []),
        )
        if self._status_callback:
            try:
                self._status_callback(status)
            except Exception:  # pragma: no cover - UI callback guard
                logger.debug("Status callback failed", exc_info=True)

    def _set_offline(self, message: str) -> None:
        self._online = False
        self._notify_status(error=message)

    def _sync_once(self, *, force_pull: bool = False) -> None:
        try:
            changed = db.refresh_from_remote()
        except SheetsClientError as exc:
            logger.warning("Sheets refresh failed: %s", exc)
            self._set_offline(str(exc))
            raise
        self._online = db.is_online()
        if not self._online:
            self._set_offline(db.last_sync_error() or "Unable to connect")
            return
        if changed or force_pull:
            self._last_sync = _utc_now_iso()
            self._notify_status(message="Data updated")
        else:
            self._notify_status()

    def _on_item_upsert(self, item_id: str) -> None:
        self._wake_event.set()
        self._pending = 0


__all__ = ["InventorySyncManager", "InventoryStatus"]
