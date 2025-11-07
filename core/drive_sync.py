"""Google Drive synchronisation for the RugBase SQLite database."""
from __future__ import annotations

import json
import os
import platform
import shutil
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

import db
from core import drive_api
from core.hash import file_sha256

DB_FILENAME = "rugbase.db"
ROOT_FOLDER_ID = "1rM1Ev9BdY_hhNOTdJgaRwziomVmNrLfq"
CHANGELOG_FOLDER_NAME = "RugBase_Changelog"
BACKUPS_FOLDER_NAME = "RugBase_Backups"
SETTINGS_FILENAME = "drive_sync_settings.json"
DEFAULT_POLL_INTERVAL = 30
TOKEN_FILENAME = "token.json"

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class SyncConfigurationError(RuntimeError):
    """Raised when Drive synchronisation has not been configured."""


@dataclass
class SyncResult:
    """Summary of a synchronisation pass."""

    action: str
    message: str
    new_conflicts: int = 0
    total_conflicts: int = 0
    last_sync: Optional[str] = None


def _now() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)


def _format_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(ISO_FORMAT)


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value).astimezone(timezone.utc)
    except ValueError:
        return None


def _default_token_path() -> str:
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return os.path.join(local_app_data, "RugBase", TOKEN_FILENAME)
    home = Path.home()
    return str(home / ".rugbase" / TOKEN_FILENAME)


def _settings_path() -> Path:
    return Path(db.resource_path(SETTINGS_FILENAME))


def _default_settings() -> Dict[str, object]:
    return {
        "client_secret_path": "",
        "token_path": _default_token_path(),
        "poll_interval": DEFAULT_POLL_INTERVAL,
        "node_name": platform.node() or "RugBaseNode",
        "last_local_hash": None,
        "last_remote_hash": None,
        "remote_file_id": None,
        "remote_modified_time": None,
        "last_sync_time": None,
        "changelog_folder_id": None,
        "backups_folder_id": None,
        "root_folder_id": ROOT_FOLDER_ID,
    }


def load_settings() -> Dict[str, object]:
    path = _settings_path()
    defaults = _default_settings()
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            defaults.update(data)
    else:
        save_settings(defaults)
    if not defaults.get("token_path"):
        defaults["token_path"] = _default_token_path()
    defaults.setdefault("root_folder_id", ROOT_FOLDER_ID)
    return defaults


def save_settings(settings: Dict[str, object]) -> None:
    path = _settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(settings, handle, indent=2, ensure_ascii=False)


def get_poll_interval() -> int:
    settings = load_settings()
    try:
        interval = int(settings.get("poll_interval", DEFAULT_POLL_INTERVAL))
    except (TypeError, ValueError):
        interval = DEFAULT_POLL_INTERVAL
    return max(interval, 1)


def _ensure_configured(settings: Dict[str, object]) -> None:
    secret_path = settings.get("client_secret_path")
    if not secret_path or not os.path.exists(str(secret_path)):
        raise SyncConfigurationError("Google Drive client secret is not configured.")


def _create_service(settings: Dict[str, object]):
    token_path = settings.get("token_path") or _default_token_path()
    return drive_api.init_client(str(settings["client_secret_path"]), str(token_path), drive_api.DEFAULT_SCOPES)


def _ensure_structure(service) -> Tuple[str, str, str]:
    root_id = ROOT_FOLDER_ID
    service.files().get(fileId=root_id, fields="id").execute()
    changelog_id = drive_api.ensure_folder(service, CHANGELOG_FOLDER_NAME, parent_id=root_id)
    backups_id = drive_api.ensure_folder(service, BACKUPS_FOLDER_NAME, parent_id=root_id)
    return root_id, changelog_id, backups_id


def test_connection(candidate_settings: Dict[str, object]) -> Dict[str, str]:
    settings = load_settings()
    settings.update(candidate_settings)
    _ensure_configured(settings)
    service = _create_service(settings)
    root_id, changelog_id, backups_id = _ensure_structure(service)
    return {"root": root_id, "changelog": changelog_id, "backups": backups_id}


class DriveSync:
    """Synchronise the RugBase SQLite database file with Google Drive."""

    def __init__(self, db_path: Optional[str] = None) -> None:
        self.db_path = Path(db_path or db.DB_PATH)
        self._lock = threading.Lock()
        self._service = None
        self._structure: Optional[Dict[str, str]] = None

    def _reload_settings(self) -> Dict[str, object]:
        return load_settings()

    def _ensure_client(self, settings: Dict[str, object]):
        with self._lock:
            if self._service is None:
                self._service = _create_service(settings)
        root_id, changelog_id, backups_id = _ensure_structure(self._service)
        structure = {"root": root_id, "changelog": changelog_id, "backups": backups_id}
        if self._structure != structure:
            self._structure = structure
        updated = False
        if settings.get("root_folder_id") != root_id:
            settings["root_folder_id"] = root_id
            updated = True
        if settings.get("changelog_folder_id") != changelog_id:
            settings["changelog_folder_id"] = changelog_id
            updated = True
        if settings.get("backups_folder_id") != backups_id:
            settings["backups_folder_id"] = backups_id
            updated = True
        if updated:
            save_settings(settings)
        return self._service, structure

    def _fetch_remote_metadata(self, service) -> Optional[Dict[str, object]]:
        query = (
            f"name = '{DB_FILENAME}' and '{ROOT_FOLDER_ID}' in parents and trashed = false"
        )
        response = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="files(id, name, modifiedTime, appProperties)",
                pageSize=1,
            )
            .execute()
        )
        files = response.get("files", [])
        if not files:
            return None
        return files[0]

    def _remote_hash(self, metadata: Dict[str, object], service) -> Tuple[str, Dict[str, object]]:
        properties = metadata.get("appProperties") or {}
        existing_hash = properties.get("sha256")
        if existing_hash:
            return existing_hash, metadata
        file_id = metadata["id"]
        raw_bytes = drive_api.download_file(service, file_id)
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            temp_file.write(raw_bytes)
            temp_file.flush()
        finally:
            temp_file.close()
        computed_hash = file_sha256(temp_file.name)
        service.files().update(
            fileId=file_id,
            body={"appProperties": {"sha256": computed_hash}},
            fields="id, appProperties",
        ).execute()
        metadata["appProperties"] = {"sha256": computed_hash}
        os.remove(temp_file.name)
        return computed_hash, metadata

    def _upload_local(self, service, file_id: Optional[str], structure: Dict[str, str], settings: Dict[str, object]) -> Tuple[str, Dict[str, object]]:
        local_hash = file_sha256(self.db_path)
        media = drive_api.MediaFileUpload(str(self.db_path), mimetype="application/octet-stream", resumable=False)
        body = {"name": DB_FILENAME, "appProperties": {"sha256": local_hash}}
        if file_id:
            updated = (
                service.files()
                .update(fileId=file_id, body=body, media_body=media, fields="id, modifiedTime, appProperties")
                .execute()
            )
        else:
            body["parents"] = [structure["root"]]
            updated = (
                service.files()
                .create(body=body, media_body=media, fields="id, modifiedTime, appProperties")
                .execute()
            )
        settings["last_local_hash"] = local_hash
        settings["last_remote_hash"] = local_hash
        settings["remote_file_id"] = updated["id"]
        settings["remote_modified_time"] = updated.get("modifiedTime")
        settings["last_sync_time"] = _format_iso(_now())
        save_settings(settings)
        return local_hash, updated

    def _download_remote(self, service, file_id: str, settings: Dict[str, object]) -> Tuple[str, Dict[str, object]]:
        raw_bytes = drive_api.download_file(service, file_id)
        temp_path = Path(tempfile.gettempdir()) / f"rugbase_download_{os.getpid()}"
        with open(temp_path, "wb") as handle:
            handle.write(raw_bytes)
        temp_hash = file_sha256(temp_path)
        shutil.move(temp_path, self.db_path)
        metadata = (
            service.files()
            .update(
                fileId=file_id,
                body={"appProperties": {"sha256": temp_hash}},
                fields="id, modifiedTime, appProperties",
            )
            .execute()
        )
        settings["last_local_hash"] = temp_hash
        settings["last_remote_hash"] = temp_hash
        settings["remote_file_id"] = file_id
        settings["remote_modified_time"] = metadata.get("modifiedTime")
        settings["last_sync_time"] = _format_iso(_now())
        save_settings(settings)
        return temp_hash, metadata

    def _copy_to_backups(self, service, file_id: str, structure: Dict[str, str]) -> None:
        timestamp = _now().strftime("%Y%m%dT%H%M%SZ")
        backup_name = f"rugbase_conflict_{timestamp}.db"
        service.files().copy(
            fileId=file_id,
            body={"name": backup_name, "parents": [structure["backups"]]},
            fields="id",
        ).execute()

    def _log_conflict(self, settings: Dict[str, object], local_hash: Optional[str], remote_hash: Optional[str], local_mtime: Optional[datetime], remote_mtime: Optional[datetime]) -> None:
        payload = {
            "local_hash": local_hash,
            "remote_hash": remote_hash,
            "local_mtime": _format_iso(local_mtime) if local_mtime else None,
            "remote_mtime": _format_iso(remote_mtime) if remote_mtime else None,
            "node": settings.get("node_name"),
        }
        db.log_conflict(DB_FILENAME, None, "Detected conflicting database versions", payload)

    def sync_once(self) -> SyncResult:
        settings = self._reload_settings()
        _ensure_configured(settings)
        service, structure = self._ensure_client(settings)

        previous_conflicts = db.count_conflicts(resolved=False)
        local_exists = self.db_path.exists()
        local_mtime = datetime.fromtimestamp(self.db_path.stat().st_mtime, timezone.utc) if local_exists else None
        local_hash = file_sha256(self.db_path) if local_exists else None

        metadata = self._fetch_remote_metadata(service)
        if not metadata:
            if not local_exists:
                message = "No database found locally or on Drive."
                return SyncResult(action="noop", message=message, last_sync=settings.get("last_sync_time"), total_conflicts=previous_conflicts)
            self._upload_local(service, None, structure, settings)
            message = "Uploaded local database to Google Drive."
            total_conflicts = db.count_conflicts(resolved=False)
            return SyncResult(
                action="upload",
                message=message,
                new_conflicts=max(total_conflicts - previous_conflicts, 0),
                total_conflicts=total_conflicts,
                last_sync=settings.get("last_sync_time"),
            )

        remote_hash, metadata = self._remote_hash(metadata, service)
        remote_mtime = _parse_iso(metadata.get("modifiedTime"))
        file_id = metadata.get("id")
        settings["remote_file_id"] = file_id

        last_local_hash = settings.get("last_local_hash")
        last_remote_hash = settings.get("last_remote_hash")
        local_changed = local_hash is not None and last_local_hash and local_hash != last_local_hash
        remote_changed = remote_hash and last_remote_hash and remote_hash != last_remote_hash

        if not local_exists:
            downloaded_hash, _ = self._download_remote(service, file_id, settings)
            total_conflicts = db.count_conflicts(resolved=False)
            return SyncResult(
                action="download",
                message="Downloaded database from Google Drive.",
                new_conflicts=max(total_conflicts - previous_conflicts, 0),
                total_conflicts=total_conflicts,
                last_sync=settings.get("last_sync_time"),
            )

        if local_hash == remote_hash:
            settings["last_local_hash"] = local_hash
            settings["last_remote_hash"] = remote_hash
            settings["remote_modified_time"] = metadata.get("modifiedTime")
            settings["last_sync_time"] = _format_iso(_now())
            save_settings(settings)
            total_conflicts = db.count_conflicts(resolved=False)
            return SyncResult(
                action="noop",
                message="Local and Drive databases are already in sync.",
                new_conflicts=max(total_conflicts - previous_conflicts, 0),
                total_conflicts=total_conflicts,
                last_sync=settings.get("last_sync_time"),
            )

        if local_changed and remote_changed:
            self._copy_to_backups(service, file_id, structure)
            self._upload_local(service, file_id, structure, settings)
            self._log_conflict(settings, local_hash, remote_hash, local_mtime, remote_mtime)
            total_conflicts = db.count_conflicts(resolved=False)
            new_conflicts = max(total_conflicts - previous_conflicts, 0)
            return SyncResult(
                action="conflict",
                message="Conflict detected. Local database uploaded and Drive copy backed up.",
                new_conflicts=new_conflicts,
                total_conflicts=total_conflicts,
                last_sync=settings.get("last_sync_time"),
            )

        if remote_mtime and (not local_mtime or remote_mtime > local_mtime):
            downloaded_hash, _ = self._download_remote(service, file_id, settings)
            total_conflicts = db.count_conflicts(resolved=False)
            return SyncResult(
                action="download",
                message="Downloaded newer database from Google Drive.",
                new_conflicts=max(total_conflicts - previous_conflicts, 0),
                total_conflicts=total_conflicts,
                last_sync=settings.get("last_sync_time"),
            )

        self._upload_local(service, file_id, structure, settings)
        total_conflicts = db.count_conflicts(resolved=False)
        return SyncResult(
            action="upload",
            message="Uploaded local database to Google Drive.",
            new_conflicts=max(total_conflicts - previous_conflicts, 0),
            total_conflicts=total_conflicts,
            last_sync=settings.get("last_sync_time"),
        )

    def backup_local(self) -> str:
        settings = self._reload_settings()
        _ensure_configured(settings)
        service, structure = self._ensure_client(settings)
        if not self.db_path.exists():
            raise FileNotFoundError("Local database file does not exist.")
        timestamp = _now().strftime("%Y%m%dT%H%M%SZ")
        archive_name = f"rugbase_backup_{timestamp}.db"
        media = drive_api.MediaFileUpload(str(self.db_path), mimetype="application/octet-stream", resumable=False)
        service.files().create(
            body={"name": archive_name, "parents": [structure["backups"]]},
            media_body=media,
            fields="id",
        ).execute()
        settings["last_sync_time"] = _format_iso(_now())
        save_settings(settings)
        return archive_name


__all__ = [
    "DriveSync",
    "SyncResult",
    "SyncConfigurationError",
    "DEFAULT_POLL_INTERVAL",
    "load_settings",
    "save_settings",
    "test_connection",
    "get_poll_interval",
]
