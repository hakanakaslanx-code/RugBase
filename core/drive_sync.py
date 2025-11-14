"""Google Drive synchronisation for the RugBase SQLite database."""
from __future__ import annotations

import importlib
import json
import logging
import os
import platform
import shutil
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, NoReturn
import tempfile
from stat import S_IRUSR, S_IWUSR
import site
import sys

import db
from core import app_paths, deps_bootstrap, drive_api
from core.hash import file_sha256
import settings

logger = logging.getLogger(__name__)

DEPENDENCY_ERROR_MESSAGE = "The sync module is missing; please reinstall the distribution package."


def _raise_dependency_error() -> NoReturn:
    missing = deps_bootstrap.missing_dependencies()
    details = ", ".join(missing) if missing else "unknown"
    logger.error("[Deps] Google modules missing: %s", details)
    message = DEPENDENCY_ERROR_MESSAGE
    if missing:
        message += f" Missing packages: {details}"
    raise SyncConfigurationError(message)


def _ensure_dependencies_ready() -> None:
    if deps_bootstrap.ensure_google_deps():
        return
    _raise_dependency_error()


def _iter_site_directories() -> Iterable[str]:
    seen: set[str] = set()
    candidates: list[str] = []

    try:
        for path in site.getsitepackages():
            if path:
                candidates.append(path)
    except Exception:  # pragma: no cover - environment dependent
        logger.debug("[Deps] Global site-packages location could not be read", exc_info=True)

    try:
        user_site = site.getusersitepackages()
    except Exception:  # pragma: no cover - environment dependent
        logger.debug("[Deps] User site-packages location could not be read", exc_info=True)
        user_site = None
    if isinstance(user_site, str) and user_site:
        candidates.append(user_site)

    python_path = os.environ.get("PYTHONPATH")
    if python_path:
        for part in python_path.split(os.pathsep):
            if part:
                candidates.append(part)

    for raw in candidates:
        normalised = os.path.abspath(raw)
        if normalised and normalised not in seen:
            seen.add(normalised)
            yield normalised


def _refresh_site_packages() -> None:
    for directory in _iter_site_directories():
        if not os.path.isdir(directory):
            continue
        try:
            site.addsitedir(directory)
        except Exception:  # pragma: no cover - defensive
            logger.debug(
                "[Deps] Failed to register site-packages directory %s", directory, exc_info=True
            )
        else:
            if directory not in sys.path:
                sys.path.append(directory)
            logger.debug("[Deps] site-packages directory registered: %s", directory)

    importlib.invalidate_caches()


app_paths.ensure_app_structure()

SECURITY_NOTE = (
    "IMPORTANT: When storing shared service account keys (private key/id), ensure"
    " they are encrypted or file permissions are restricted. Keys must never be"
    " committed to GitHub. If a key has already been exposed, rotate it immediately."
)

logger.warning("[Drive] %s", SECURITY_NOTE)

SETTINGS_FILENAME = "drive_sync_settings.json"
MAIN_SETTINGS_SECTION = "drive_sync"
DB_FILENAME = "rugbase.db"
CHANGELOG_FOLDER_NAME = "RugBase_Changelog"
BACKUPS_FOLDER_NAME = "RugBase_Backups"
DEFAULT_POLL_INTERVAL = 30
TOKEN_FILENAME = "token.json"
CREDENTIALS_FILENAME = "service_account.json"


def _drive_settings_path() -> Path:
    return app_paths.config_path(SETTINGS_FILENAME)


def _load_json_mapping(path: Path, *, warn: bool) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        log = logger.warning if warn else logger.debug
        log("[Drive] Settings file %s could not be read: %s", path, exc, exc_info=not warn)
        return {}
    if isinstance(payload, Mapping):
        return dict(payload)
    return {}


def _load_main_drive_config() -> Dict[str, str]:
    settings_path = Path(settings.DEFAULT_SETTINGS_PATH)
    payload = _load_json_mapping(settings_path, warn=False)
    section = payload.get(MAIN_SETTINGS_SECTION, {}) if isinstance(payload, Mapping) else {}
    if not isinstance(section, Mapping):
        return {}
    config: Dict[str, str] = {}
    for key, value in section.items():
        if value in (None, ""):
            continue
        config[str(key)] = str(value)
    return config


def _load_legacy_drive_config() -> Dict[str, str]:
    payload = _load_json_mapping(_drive_settings_path(), warn=True)
    return {str(key): str(value) for key, value in payload.items() if value not in (None, "")}


def _load_drive_config() -> Dict[str, str]:
    config: Dict[str, str] = {}
    config.update(_load_main_drive_config())
    config.update(_load_legacy_drive_config())
    return config


def _drive_setting(key: str, env_var: str, default: str = "") -> str:
    value = os.getenv(env_var)
    if value:
        return value
    return _DRIVE_CONFIG.get(key, default)


_DRIVE_CONFIG: Dict[str, str] = _load_drive_config()

ROOT_FOLDER_ID = _drive_setting("root_folder_id", "RUGBASE_ROOT_FOLDER_ID")
DEFAULT_SERVICE_ACCOUNT_EMAIL = _drive_setting("service_account_email", "RUGBASE_SERVICE_ACCOUNT_EMAIL")
DEFAULT_SPREADSHEET_URL = _drive_setting("spreadsheet_url", "RUGBASE_SPREADSHEET_URL")
DEFAULT_PRIVATE_KEY_ID = _drive_setting("private_key_id", "RUGBASE_PRIVATE_KEY_ID")
STATUS_CONNECTED = "connected"
STATUS_OFFLINE = "offline"
STATUS_REAUTHORISE = "reauthorize"
STATUS_CONFLICT = "conflict"

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class SyncConfigurationError(RuntimeError):
    """Raised when Drive synchronisation has not been configured."""


class SyncAuthenticationRequired(RuntimeError):
    """Raised when OAuth credentials must be refreshed by the user."""


class SyncOfflineError(RuntimeError):
    """Raised when synchronisation cannot complete due to connectivity issues."""


@dataclass
class SyncResult:
    """Summary of a synchronisation pass."""

    action: str
    message: str
    new_conflicts: int = 0
    total_conflicts: int = 0
    last_sync: Optional[str] = None
    status: str = STATUS_CONNECTED
    requires_resolution: bool = False
    backup_path: Optional[str] = None


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
    token_file = app_paths.tokens_path(TOKEN_FILENAME)
    return str(token_file)


def _resolve_token_path(token_path: Optional[str]) -> str:
    default_path = Path(_default_token_path())
    if token_path:
        expanded = Path(os.path.expanduser(str(token_path))).resolve()
        parts = [part.lower() for part in expanded.parts]
        if "desktop" in parts:
            logger.warning(
                "[Drive] Token path cannot be on the desktop; default location will be used"
            )
        else:
            default_path = expanded
    default_path.parent.mkdir(parents=True, exist_ok=True)
    return str(default_path)


def _ensure_token_directory(token_path: str) -> str:
    resolved_path = _resolve_token_path(token_path)
    directory = os.path.dirname(resolved_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    return resolved_path


def _credentials_storage_path(filename: str = CREDENTIALS_FILENAME) -> Path:
    destination = app_paths.credentials_path(filename)
    destination.parent.mkdir(parents=True, exist_ok=True)
    return destination


def _default_client_secret_path() -> str:
    storage = _credentials_storage_path()
    return str(storage)


def _normalise_client_secret_path(candidate: Optional[str]) -> str:
    destination = _credentials_storage_path()
    if not candidate:
        return str(destination) if destination.exists() else ""

    source_path = Path(os.path.expanduser(str(candidate))).resolve()
    if destination.exists() and source_path == destination:
        return str(destination)
    if source_path == destination and not source_path.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        return str(destination)
    if not source_path.exists():
        raise FileNotFoundError(f"Client secret file not found: {source_path}")
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination)
        try:
            os.chmod(destination, S_IRUSR | S_IWUSR)
        except OSError:
            logger.debug(
                "[Drive] File permissions could not be set: %s", destination, exc_info=True
            )
    except OSError as exc:
        raise RuntimeError(
            f"Unable to copy client secret to {destination}: {exc}"
        ) from exc
    logger.info("[Drive] Service account file copied to %s", destination)
    return str(destination)


def service_account_storage_path() -> str:
    """Return the expected storage path for the service account credentials."""

    return str(_credentials_storage_path())


def _settings_path() -> Path:
    return app_paths.config_path(SETTINGS_FILENAME)


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
        "service_account_email": DEFAULT_SERVICE_ACCOUNT_EMAIL,
        "spreadsheet_url": DEFAULT_SPREADSHEET_URL,
        "private_key_id": DEFAULT_PRIVATE_KEY_ID,
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
    original_token_path = str(defaults.get("token_path"))
    resolved_token_path = _ensure_token_directory(original_token_path)
    if resolved_token_path != original_token_path:
        defaults["token_path"] = resolved_token_path
        save_settings(defaults)
    else:
        defaults["token_path"] = resolved_token_path
    secret_candidate = str(defaults.get("client_secret_path") or "")
    if not secret_candidate:
        normalised_secret = _default_client_secret_path()
    else:
        try:
            normalised_secret = _normalise_client_secret_path(secret_candidate)
        except FileNotFoundError:
            logger.warning(
                "[Drive] Specified service account file not found: %s", secret_candidate
            )
            normalised_secret = _default_client_secret_path()
        except RuntimeError as exc:
            logger.error("[Drive] Failed to save service account: %s", exc)
            raise
    if defaults.get("client_secret_path") != normalised_secret:
        defaults["client_secret_path"] = normalised_secret
        save_settings(defaults)
    else:
        defaults["client_secret_path"] = normalised_secret
    defaults.setdefault("root_folder_id", ROOT_FOLDER_ID)
    defaults.setdefault("service_account_email", DEFAULT_SERVICE_ACCOUNT_EMAIL)
    defaults.setdefault("spreadsheet_url", DEFAULT_SPREADSHEET_URL)
    defaults.setdefault("private_key_id", DEFAULT_PRIVATE_KEY_ID)
    return defaults


def save_settings(settings: Dict[str, object]) -> None:
    path = _settings_path()
    token_path = _ensure_token_directory(str(settings.get("token_path") or _default_token_path()))
    settings["token_path"] = token_path
    try:
        secret_path = _normalise_client_secret_path(settings.get("client_secret_path"))
    except FileNotFoundError as exc:
        raise RuntimeError(str(exc)) from exc
    except RuntimeError:
        raise
    else:
        settings["client_secret_path"] = secret_path
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
    _ensure_dependencies_ready()
    token_path = _ensure_token_directory(str(settings.get("token_path") or _default_token_path()))
    if settings.get("token_path") != token_path:
        settings["token_path"] = token_path
        save_settings(settings)
    try:
        return drive_api.init_client(
            str(settings["client_secret_path"]),
            str(token_path),
            drive_api.DEFAULT_SCOPES,
        )
    except drive_api.AuthenticationError as exc:
        message = f"{exc}. Rotate the keys."
        raise SyncAuthenticationRequired(message) from exc


def _ensure_structure(service) -> Tuple[str, str, str]:
    root_id = ROOT_FOLDER_ID
    service.files().get(fileId=root_id, fields="id").execute()
    changelog_id = drive_api.ensure_folder(service, CHANGELOG_FOLDER_NAME, parent_id=root_id)
    backups_id = drive_api.ensure_folder(service, BACKUPS_FOLDER_NAME, parent_id=root_id)
    return root_id, changelog_id, backups_id


def test_connection(candidate_settings: Dict[str, object]) -> Dict[str, str]:
    settings = load_settings()
    settings.update(candidate_settings)
    _ensure_dependencies_ready()
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
        self._conflict_pending = False
        self._pending_conflict_backup: Optional[Path] = None

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

    def _backup_local_copy(self) -> Optional[Path]:
        if not self.db_path.exists():
            return None
        backup_dir = app_paths.ensure_directory(app_paths.BACKUP_DIR)
        timestamp = _now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"rugbase_{timestamp}.db"
        try:
            shutil.copy2(self.db_path, backup_path)
        except OSError as exc:
            logger.error("Failed to back up local database to %s: %s", backup_path, exc)
            return None
        logger.info("[Drive] Local database backed up: %s", backup_path)
        return backup_path

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
        if self._conflict_pending:
            total_conflicts = db.count_conflicts(resolved=False)
            return SyncResult(
                action="conflict",
                message="Conflict resolution pending.",
                total_conflicts=total_conflicts,
                last_sync=settings.get("last_sync_time"),
                status=STATUS_CONFLICT,
                requires_resolution=True,
                backup_path=str(self._pending_conflict_backup) if self._pending_conflict_backup else None,
            )
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
            backup_path = self._backup_local_copy()
            try:
                self._copy_to_backups(service, file_id, structure)
            except Exception:  # pragma: no cover - defensive
                logger.warning("[Drive] Remote conflict backup could not be created", exc_info=True)
            self._log_conflict(settings, local_hash, remote_hash, local_mtime, remote_mtime)
            self._conflict_pending = True
            self._pending_conflict_backup = backup_path
            total_conflicts = db.count_conflicts(resolved=False)
            new_conflicts = max(total_conflicts - previous_conflicts, 0)
            return SyncResult(
                action="conflict",
                message="Conflict detected. Choose which copy should be kept.",
                new_conflicts=new_conflicts,
                total_conflicts=total_conflicts,
                last_sync=settings.get("last_sync_time"),
                status=STATUS_CONFLICT,
                requires_resolution=True,
                backup_path=str(backup_path) if backup_path else None,
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

    def resolve_conflict(self, prefer_local: bool) -> SyncResult:
        settings = self._reload_settings()
        _ensure_configured(settings)
        service, structure = self._ensure_client(settings)
        metadata = self._fetch_remote_metadata(service)
        if not metadata:
            raise RuntimeError("No remote database found to resolve the conflict.")
        file_id = metadata.get("id")
        if not file_id:
            raise RuntimeError("Remote database identifier is missing.")
        if prefer_local:
            self._upload_local(service, file_id, structure, settings)
            action = "upload"
            message = "Local database uploaded to Google Drive."
        else:
            self._download_remote(service, file_id, settings)
            action = "download"
            message = "Remote database restored locally."
        self._conflict_pending = False
        self._pending_conflict_backup = None
        total_conflicts = db.count_conflicts(resolved=False)
        return SyncResult(
            action=action,
            message=message,
            total_conflicts=total_conflicts,
            last_sync=settings.get("last_sync_time"),
            status=STATUS_CONNECTED,
        )

    def restore_remote(self) -> SyncResult:
        settings = self._reload_settings()
        _ensure_configured(settings)
        service, _ = self._ensure_client(settings)
        metadata = self._fetch_remote_metadata(service)
        if not metadata:
            raise FileNotFoundError("No remote database is available to restore.")
        file_id = metadata.get("id")
        if not file_id:
            raise RuntimeError("Remote database identifier is missing.")
        self._download_remote(service, file_id, settings)
        self._conflict_pending = False
        self._pending_conflict_backup = None
        total_conflicts = db.count_conflicts(resolved=False)
        return SyncResult(
            action="restore",
            message="Remote database restored locally.",
            total_conflicts=total_conflicts,
            last_sync=settings.get("last_sync_time"),
            status=STATUS_CONNECTED,
        )

    def reset_credentials(self) -> None:
        settings = self._reload_settings()
        token_path = settings.get("token_path")
        if token_path and os.path.exists(str(token_path)):
            try:
                os.remove(str(token_path))
            except OSError as exc:
                raise RuntimeError(f"Unable to remove token file: {exc}") from exc
        settings["last_local_hash"] = None
        settings["last_remote_hash"] = None
        settings["remote_file_id"] = None
        settings["remote_modified_time"] = None
        settings["last_sync_time"] = None
        save_settings(settings)
        with self._lock:
            self._service = None
        self._conflict_pending = False
        self._pending_conflict_backup = None


__all__ = [
    "DriveSync",
    "SyncResult",
    "SyncConfigurationError",
    "SyncAuthenticationRequired",
    "SyncOfflineError",
    "DEFAULT_POLL_INTERVAL",
    "load_settings",
    "save_settings",
    "test_connection",
    "get_poll_interval",
    "service_account_storage_path",
    "STATUS_CONNECTED",
    "STATUS_OFFLINE",
    "STATUS_REAUTHORISE",
    "STATUS_CONFLICT",
    "DEFAULT_SERVICE_ACCOUNT_EMAIL",
    "DEFAULT_SPREADSHEET_URL",
    "DEFAULT_PRIVATE_KEY_ID",
]
