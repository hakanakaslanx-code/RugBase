"""Synchronization helpers for RugBase using Google Drive."""
from __future__ import annotations

import json
import os
import platform
import shutil
import sqlite3
import tempfile
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import db
from core import drive_api


DEFAULT_ROOT_NAME = "RugBaseSync"
DEFAULT_POLL_INTERVAL = 300
SYNC_SETTINGS_FILENAME = "sync_settings.json"
SYNC_SETTINGS_PATH = db.resource_path(SYNC_SETTINGS_FILENAME)
ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DB_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


class SyncConfigurationError(RuntimeError):
    """Raised when Drive synchronisation has not been configured."""


@dataclass
class SyncResult:
    applied: int
    processed: int
    new_conflicts: int
    total_conflicts: int
    last_sync: Optional[str]


def _default_settings() -> Dict[str, Any]:
    return {
        "client_secret_path": "",
        "token_path": db.resource_path("token.json"),
        "root_folder_id": "",
        "root_folder_name": DEFAULT_ROOT_NAME,
        "changelog_folder_id": "",
        "backups_folder_id": "",
        "last_sync_time": None,
        "poll_interval": DEFAULT_POLL_INTERVAL,
        "node_name": platform.node() or "RugBaseNode",
    }


def load_settings() -> Dict[str, Any]:
    settings = _default_settings()
    if os.path.exists(SYNC_SETTINGS_PATH):
        with open(SYNC_SETTINGS_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
            if isinstance(data, dict):
                settings.update(data)
    else:
        save_settings(settings)

    if not settings.get("token_path"):
        settings["token_path"] = db.resource_path("token.json")
    return settings


def save_settings(settings: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(SYNC_SETTINGS_PATH) or ".", exist_ok=True)
    with open(SYNC_SETTINGS_PATH, "w", encoding="utf-8") as handle:
        json.dump(settings, handle, indent=2, ensure_ascii=False)


def _is_configured(settings: Dict[str, Any]) -> bool:
    secret = settings.get("client_secret_path")
    if not secret or not os.path.exists(secret):
        return False
    token_path = settings.get("token_path") or db.resource_path("token.json")
    if not token_path:
        return False
    root_hint = settings.get("root_folder_id") or settings.get("root_folder_name")
    return bool(root_hint)


def _require_configured(settings: Dict[str, Any]) -> None:
    if not _is_configured(settings):
        raise SyncConfigurationError(
            "Google Drive synchronisation is not configured. Open Sync Settings to connect."
        )


def _ensure_service(
    settings: Dict[str, Any],
    *,
    persist: bool = True,
):
    working = dict(settings)
    token_path = working.get("token_path") or db.resource_path("token.json")
    working["token_path"] = token_path

    service = drive_api.init_client(working["client_secret_path"], token_path, drive_api.DEFAULT_SCOPES)
    structure = drive_api.ensure_structure(
        service,
        working.get("root_folder_id") or None,
        working.get("root_folder_name") or DEFAULT_ROOT_NAME,
    )

    updated = False
    for key, folder_key in (
        ("root_folder_id", "root"),
        ("changelog_folder_id", "changelog"),
        ("backups_folder_id", "backups"),
    ):
        if working.get(key) != structure[folder_key]:
            working[key] = structure[folder_key]
            updated = True

    if persist and (updated or working != settings):
        save_settings(working)

    return service, structure, working


def _utcnow_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


def _format_db_timestamp(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(DB_TIMESTAMP_FORMAT)


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    cleaned = value
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_db_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    for fmt in (DB_TIMESTAMP_FORMAT, "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return _parse_timestamp(value)


def _load_item_snapshot(item_id: str) -> Optional[Dict[str, Any]]:
    columns = [column for column, _ in db.TABLE_COLUMNS]
    query = f"SELECT {', '.join(columns)} FROM item WHERE item_id = ?"
    with db.get_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(query, (item_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def _build_change_payload(change_type: str, item: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = _utcnow_iso()
    payload: Dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "type": change_type,
        "timestamp": timestamp,
        "node": settings.get("node_name") or platform.node() or "RugBaseNode",
    }

    if change_type == "upsert":
        payload["item_id"] = item["item_id"]
        payload["data"] = item
    elif change_type == "delete":
        payload["item_id"] = item["item_id"]
        payload["data"] = {
            "item_id": item["item_id"],
            "updated_at": item.get("updated_at"),
            "status": "deleted",
        }
    else:
        payload["data"] = item
    return payload


def push_change(change: Dict[str, Any], settings: Optional[Dict[str, Any]] = None) -> str:
    settings = dict(settings or load_settings())
    _require_configured(settings)
    service, structure, working = _ensure_service(settings)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"change_{timestamp}_{uuid.uuid4().hex}.json"
    drive_api.upload_json(service, structure["changelog"], filename, change)

    if working != settings:
        save_settings(working)
    return filename


def handle_local_upsert(item_id: str) -> None:
    settings = load_settings()
    _require_configured(settings)
    snapshot = _load_item_snapshot(item_id)
    if not snapshot:
        return
    change = _build_change_payload("upsert", snapshot, settings)
    push_change(change, settings=settings)


def handle_local_delete(item_id: str) -> None:
    settings = load_settings()
    _require_configured(settings)
    snapshot = _load_item_snapshot(item_id)
    if not snapshot:
        snapshot = {"item_id": item_id, "updated_at": None, "status": "deleted"}
    change = _build_change_payload("delete", snapshot, settings)
    push_change(change, settings=settings)


def test_connection(candidate_settings: Dict[str, Any]) -> Dict[str, str]:
    settings = load_settings()
    settings.update(candidate_settings)
    _require_configured(settings)
    _, structure, _ = _ensure_service(settings, persist=False)
    return structure


def _apply_upsert_change(change: Dict[str, Any], change_file: str) -> Tuple[bool, bool]:
    data = change.get("data") or {}
    item_id = data.get("item_id") or change.get("item_id")
    if not item_id:
        db.log_conflict(change_file, None, "Missing item_id for upsert", change)
        return False, True

    remote_updated_at = _parse_timestamp(data.get("updated_at") or change.get("timestamp"))
    existing = _load_item_snapshot(item_id)
    existing_updated_at = _parse_db_timestamp(existing.get("updated_at")) if existing else None

    if existing_updated_at and remote_updated_at and remote_updated_at <= existing_updated_at:
        if remote_updated_at < existing_updated_at:
            db.log_conflict(
                change_file,
                item_id,
                "Remote change is older than the local record; keeping local version.",
                {
                    "remote": data,
                    "local_updated_at": existing.get("updated_at"),
                    "remote_updated_at": data.get("updated_at") or change.get("timestamp"),
                },
            )
            return False, True
        return False, False

    merged: Dict[str, Any] = {}
    columns = [column for column, _ in db.TABLE_COLUMNS]
    for column in columns:
        if existing and column in existing:
            merged[column] = existing[column]
        else:
            merged[column] = None

    for key, value in data.items():
        merged[key] = value
    merged["item_id"] = item_id

    if remote_updated_at:
        merged["updated_at"] = _format_db_timestamp(remote_updated_at)
    elif not merged.get("updated_at"):
        merged["updated_at"] = datetime.utcnow().strftime(DB_TIMESTAMP_FORMAT)

    if not merged.get("created_at"):
        if existing and existing.get("created_at"):
            merged["created_at"] = existing["created_at"]
        elif remote_updated_at:
            merged["created_at"] = _format_db_timestamp(remote_updated_at)
        else:
            merged["created_at"] = datetime.utcnow().strftime(DB_TIMESTAMP_FORMAT)

    columns_str = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    update_clause = ", ".join(f"{column} = excluded.{column}" for column in columns if column != "item_id")

    with db.get_connection() as conn:
        conn.execute(
            f"INSERT INTO item ({columns_str}) VALUES ({placeholders}) "
            f"ON CONFLICT(item_id) DO UPDATE SET {update_clause}",
            [merged.get(column) for column in columns],
        )
        conn.commit()

    return True, False


def _apply_delete_change(change: Dict[str, Any], change_file: str) -> Tuple[bool, bool]:
    data = change.get("data") or {}
    item_id = change.get("item_id") or data.get("item_id")
    if not item_id:
        db.log_conflict(change_file, None, "Missing item_id for delete", change)
        return False, True

    remote_updated_at = _parse_timestamp(data.get("updated_at") or change.get("timestamp"))
    snapshot = _load_item_snapshot(item_id)
    existing_updated_at = _parse_db_timestamp(snapshot.get("updated_at")) if snapshot else None

    if snapshot and existing_updated_at and remote_updated_at and remote_updated_at < existing_updated_at:
        db.log_conflict(
            change_file,
            item_id,
            "Remote delete is older than local change; ignoring delete.",
            {
                "local_updated_at": snapshot.get("updated_at"),
                "remote_updated_at": data.get("updated_at") or change.get("timestamp"),
            },
        )
        return False, True

    timestamp = _format_db_timestamp(remote_updated_at) if remote_updated_at else datetime.utcnow().strftime(
        DB_TIMESTAMP_FORMAT
    )
    with db.get_connection() as conn:
        conn.execute(
            "UPDATE item SET status = 'deleted', updated_at = ? WHERE item_id = ?",
            (timestamp, item_id),
        )
        conn.commit()

    return True, False


def _apply_stock_transaction(change: Dict[str, Any], change_file: str) -> Tuple[bool, bool]:
    txn_id = change.get("txn_id")
    if not txn_id:
        db.log_conflict(change_file, None, "Missing txn_id for stock transaction", change)
        return False, True

    if db.has_processed_stock_txn(txn_id):
        return False, False

    applied_any = False
    conflict_found = False
    operations = change.get("operations") or []
    for operation in operations:
        op_type = operation.get("type")
        if op_type == "upsert":
            applied, conflict = _apply_upsert_change(operation, change_file)
        elif op_type == "delete":
            applied, conflict = _apply_delete_change(operation, change_file)
        else:
            db.log_conflict(change_file, operation.get("item_id"), f"Unsupported nested op: {op_type}", operation)
            applied, conflict = False, True
        applied_any = applied_any or applied
        conflict_found = conflict_found or conflict

    db.record_processed_stock_txn(txn_id, change.get("timestamp"))
    return applied_any, conflict_found


def _apply_remote_change(change: Dict[str, Any], change_file: str) -> Tuple[bool, bool]:
    change_type = change.get("type")
    if change_type == "upsert":
        return _apply_upsert_change(change, change_file)
    if change_type == "delete":
        return _apply_delete_change(change, change_file)
    if change_type == "stock_txn":
        return _apply_stock_transaction(change, change_file)

    db.log_conflict(change_file, change.get("item_id"), f"Unknown change type: {change_type}", change)
    return False, True


def pull_and_apply() -> SyncResult:
    settings = load_settings()
    _require_configured(settings)

    last_sync = _parse_timestamp(settings.get("last_sync_time"))
    service, structure, working = _ensure_service(settings)

    previous_conflicts = db.count_conflicts(resolved=False)
    applied_changes = 0
    processed_files = 0
    conflict_logged = 0

    files = drive_api.list_files(service, structure["changelog"], last_sync)
    for metadata in files:
        name = metadata.get("name")
        if not name:
            continue
        if db.has_processed_change(name):
            continue

        file_id = metadata.get("id")
        try:
            raw_bytes = drive_api.download_file(service, file_id)
            change = json.loads(raw_bytes.decode("utf-8"))
        except Exception as exc:
            db.log_conflict(name, None, f"Failed to download or parse change: {exc}", metadata)
            db.record_processed_change(name, metadata.get("modifiedTime"))
            conflict_logged += 1
            processed_files += 1
            continue

        applied, conflict = _apply_remote_change(change, name)
        if applied:
            applied_changes += 1
        if conflict:
            conflict_logged += 1
        db.record_processed_change(name, metadata.get("modifiedTime"))
        processed_files += 1

    last_sync_iso = _utcnow_iso()
    working["last_sync_time"] = last_sync_iso
    save_settings(working)

    total_conflicts = db.count_conflicts(resolved=False)
    new_conflicts = max(total_conflicts - previous_conflicts, 0)

    return SyncResult(
        applied=applied_changes,
        processed=processed_files,
        new_conflicts=new_conflicts,
        total_conflicts=total_conflicts,
        last_sync=last_sync_iso,
    )


def backup_now() -> str:
    settings = load_settings()
    _require_configured(settings)
    service, structure, working = _ensure_service(settings)

    temp_dir = tempfile.mkdtemp(prefix="rugbase_backup_")
    backup_db_path = os.path.join(temp_dir, "inventory.db")
    try:
        with db.get_connection() as conn:
            try:
                conn.execute(f"VACUUM INTO '{backup_db_path}'")
            except sqlite3.OperationalError:
                dest = sqlite3.connect(backup_db_path)
                try:
                    conn.backup(dest)
                finally:
                    dest.close()

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M")
        zip_name = f"RugBase_backup_{timestamp}.zip"
        zip_path = os.path.join(temp_dir, zip_name)

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.write(backup_db_path, arcname=db.DB_FILENAME)

        drive_api.upload_file(service, structure["backups"], zip_name, zip_path, "application/zip")
        save_settings(working)
        return zip_name
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def get_poll_interval() -> int:
    settings = load_settings()
    interval = settings.get("poll_interval", DEFAULT_POLL_INTERVAL)
    try:
        return max(int(interval), 0)
    except (TypeError, ValueError):
        return DEFAULT_POLL_INTERVAL
