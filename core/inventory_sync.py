"""High-level Google Sheets synchronisation manager for RugBase."""
from __future__ import annotations

import logging
import contextlib
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Mapping, Optional

import db
from core import sheets_gateway
from settings import (
    DEFAULT_WORKSHEET_TITLE,
    GoogleSyncSettings,
    load_google_sync_settings,
)

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return (
        datetime.utcnow()
        .replace(microsecond=0)
        .replace(tzinfo=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalise_timestamp(value: Optional[str]) -> str:
    if not value:
        return _utc_now_iso()
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
    """Manage bidirectional synchronisation with Google Sheets."""

    def __init__(
        self,
        *,
        settings_provider: Callable[[], GoogleSyncSettings] = load_google_sync_settings,
        status_callback: Optional[StatusCallback] = None,
    ) -> None:
        self._settings_provider = settings_provider
        self._status_callback = status_callback
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._online = False
        self._last_sync: Optional[str] = None
        self._pending = db.count_sync_queue()
        self._settings: Optional[GoogleSyncSettings] = None
        self._listener_registered = False
        self._lock = threading.Lock()
        self._event_lock = threading.Lock()
        self._suspend_events = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._settings = self._settings_provider()
        self._register_listeners()
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self._thread = None
        self._unregister_listeners()

    def sync_now(self) -> None:
        self._wake_event.set()

    def load_initial_snapshot(self) -> bool:
        """Attempt to pull the latest sheet state into SQLite."""

        try:
            self._sync_once(force_pull=True)
        except Exception:
            logger.exception("Initial Google Sheets sync failed")
            return False
        return self._online

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _register_listeners(self) -> None:
        if self._listener_registered:
            return
        db.add_item_upsert_listener(self._on_item_upsert)
        self._listener_registered = True

    def _unregister_listeners(self) -> None:
        if not self._listener_registered:
            return
        try:
            db.remove_item_upsert_listener(self._on_item_upsert)
        except Exception:  # pragma: no cover - defensive cleanup
            logger.debug("Failed to remove sync listener", exc_info=True)
        finally:
            self._listener_registered = False

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            interval = max(30, int((self._settings or load_google_sync_settings()).sync_interval_seconds))
            triggered = self._wake_event.wait(timeout=interval)
            self._wake_event.clear()
            try:
                self._sync_once(force_pull=triggered)
            except Exception:
                logger.exception("Background sync cycle failed")
                self._set_offline("Senkronizasyon hatası")

    def _notify_status(self, *, message: Optional[str] = None, error: Optional[str] = None, conflicts: Optional[Iterable[str]] = None) -> None:
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
        settings = self._settings or self._settings_provider()
        self._settings = settings

        if not sheets_gateway.is_api_available():
            self._set_offline("Google Sheets bağımlılıkları eksik")
            return

        spreadsheet_id = settings.spreadsheet_id or sheets_gateway.SHEET_ID
        try:
            service = sheets_gateway.build_service_from_file(settings.credential_path)
        except Exception as exc:
            logger.warning("Sheets service creation failed: %s", exc)
            self._set_offline(str(exc))
            return

        conflicts: List[str] = []

        worksheet_title = self._resolve_worksheet_title(settings)

        try:
            self._push_pending(service, spreadsheet_id, worksheet_title)
            if force_pull or self._online or self._pending == 0:
                conflicts = self._pull_remote(service, spreadsheet_id, worksheet_title)
        except Exception as exc:
            logger.warning("Sheets sync cycle failed: %s", exc)
            self._set_offline(str(exc))
            return

        self._online = True
        self._last_sync = _utc_now_iso()
        self._pending = db.count_sync_queue()
        self._notify_status(conflicts=conflicts)

    # ------------------------------------------------------------------
    # Local change handling
    # ------------------------------------------------------------------
    def _on_item_upsert(self, item_id: str) -> None:
        with self._event_lock:
            if self._suspend_events:
                return
        with self._lock:
            item = db.fetch_item(item_id)
            if not item:
                return
            row = self._item_to_sheet_row(item)
            if not row:
                return
            payload = {"operation": "upsert", "row": row}
            db.enqueue_sync_job(row["RugNo"], payload)
            self._pending = db.count_sync_queue()
        self._notify_status()
        self._wake_event.set()

    def _item_to_sheet_row(self, item: Mapping[str, object]) -> Optional[Dict[str, object]]:
        rug_no = str(item.get("rug_no") or "").strip()
        if not rug_no:
            return None
        last_updated = _normalise_timestamp(str(item.get("updated_at") or ""))
        return {
            "RugNo": rug_no,
            "Collection": item.get("collection") or "",
            "Design": item.get("design") or "",
            "Ground": item.get("ground") or "",
            "Border": item.get("border") or "",
            "ASize": item.get("a_size") or "",
            "SSize": item.get("st_size") or "",
            "Area": item.get("area") or "",
            "Type": item.get("type") or "",
            "Shape": item.get("shape") or "",
            "Style": item.get("style") or "",
            "ImageFileName": item.get("image_file_name") or "",
            "Origin": item.get("origin") or "",
            "Retail": item.get("retail") or "",
            "SP": item.get("sp") or "",
            "MSRP": item.get("msrp") or "",
            "Cost": item.get("cost") or "",
            "Content": item.get("content") or "",
            "LastUpdated": last_updated,
            "Deleted": str(item.get("status") or "").lower() == "deleted",
        }

    # ------------------------------------------------------------------
    # Remote interactions
    # ------------------------------------------------------------------
    def _resolve_worksheet_title(self, settings: GoogleSyncSettings) -> str:
        title = (settings.worksheet_title or "").strip()
        if not title:
            return DEFAULT_WORKSHEET_TITLE
        return title

    def _push_pending(self, service, spreadsheet_id: str, worksheet_title: str) -> None:
        backoff_schedule = [1, 2, 4, 8, 16]
        while True:
            jobs = db.fetch_sync_queue(limit=200)
            if not jobs:
                break
            merged: Dict[str, Dict[str, object]] = {}
            timestamps: Dict[str, str] = {}
            job_ids: List[int] = []
            for job in jobs:
                payload = job.get("payload") or {}
                operation = payload.get("operation")
                if operation == "delete":
                    rug_no = str(payload.get("rug_no") or "").strip()
                    if not rug_no:
                        db.delete_sync_job(job["id"])
                        continue
                    row = {
                        "RugNo": rug_no,
                        "LastUpdated": _normalise_timestamp(payload.get("last_updated")),
                        "Deleted": True,
                    }
                else:
                    row = dict(payload.get("row") or {})
                    rug_no = str(row.get("RugNo") or "").strip()
                    if not rug_no:
                        db.delete_sync_job(job["id"])
                        continue
                    current_updated = row.get("LastUpdated")
                    row["LastUpdated"] = _normalise_timestamp(
                        current_updated if isinstance(current_updated, str) else str(current_updated or "")
                    )
                    merged[rug_no] = row
                    timestamps[rug_no] = str(row.get("LastUpdated"))
                    job_ids.append(int(job["id"]))

            if not merged:
                continue

            rows = list(merged.values())
            for attempt, delay in enumerate(backoff_schedule, start=1):
                try:
                    sheets_gateway.upsert_rows(
                        rows,
                        service=service,
                        spreadsheet_id=spreadsheet_id,
                        worksheet_title=worksheet_title,
                    )
                except Exception as exc:
                    if attempt == len(backoff_schedule):
                        for job_id in job_ids:
                            db.increment_sync_retry(job_id)
                        raise
                    time.sleep(delay)
                else:
                    for job_id in job_ids:
                        db.delete_sync_job(job_id)
                    db.mark_items_synced(timestamps)
                    break

            self._pending = db.count_sync_queue()

    def _pull_remote(self, service, spreadsheet_id: str, worksheet_title: str) -> List[str]:
        conflicts: List[str] = []
        rows = sheets_gateway.get_rows(
            service=service,
            spreadsheet_id=spreadsheet_id,
            worksheet_title=worksheet_title,
        )
        for row in rows:
            conflict = self._apply_remote_row(row)
            if conflict:
                conflicts.append(conflict)
        return conflicts

    def _apply_remote_row(self, row: Mapping[str, object]) -> Optional[str]:
        rug_no = str(row.get("RugNo") or "").strip()
        if not rug_no:
            return None
        deleted = bool(row.get("Deleted"))
        remote_updated = _normalise_timestamp(str(row.get("LastUpdated") or ""))
        local = db.fetch_item_by_rug_no(rug_no)
        local_updated = _normalise_timestamp(str(local.get("updated_at"))) if local else None
        has_local_pending = False
        if local:
            version = int(local.get("version") or 0)
            pushed = int(local.get("last_pushed_version") or 0)
            has_local_pending = version > pushed

        if deleted:
            if local:
                with self._muted_events():
                    db.delete_item(local["item_id"])
                    db.set_item_remote_timestamp(local["item_id"], remote_updated)
            return None

        should_apply = not local
        if local and remote_updated and local_updated:
            should_apply = remote_updated > local_updated
        elif local and remote_updated and not local_updated:
            should_apply = True
        elif not local:
            should_apply = True

        if not should_apply:
            return None

        payload = {
            "rug_no": rug_no,
            "collection": row.get("Collection") or "",
            "design": row.get("Design") or "",
            "ground": row.get("Ground") or "",
            "border": row.get("Border") or "",
            "a_size": row.get("ASize") or "",
            "st_size": row.get("SSize") or "",
            "area": row.get("Area"),
            "type": row.get("Type") or "",
            "shape": row.get("Shape") or "",
            "style": row.get("Style") or "",
            "image_file_name": row.get("ImageFileName") or "",
            "origin": row.get("Origin") or "",
            "retail": row.get("Retail") or "",
            "sp": row.get("SP") or "",
            "msrp": row.get("MSRP") or "",
            "cost": row.get("Cost") or "",
            "content": row.get("Content") or "",
            "updated_by": "sheets",
        }
        if local:
            payload["item_id"] = local.get("item_id")
        with self._muted_events():
            item_id, _created = db.upsert_item(payload)
            db.set_item_remote_timestamp(item_id, remote_updated)
        if has_local_pending:
            return rug_no
        return None

    @contextlib.contextmanager
    def _muted_events(self):
        with self._event_lock:
            self._suspend_events = True
        try:
            yield
        finally:
            with self._event_lock:
                self._suspend_events = False


__all__ = ["InventorySyncManager", "InventoryStatus"]
