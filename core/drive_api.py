"""Google Drive API helpers for RugBase synchronization."""
from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

try:  # pragma: no cover - optional dependency guard
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:  # pragma: no cover - handled at runtime
    build = None
    HttpError = Exception  # type: ignore
    MediaFileUpload = None
    MediaIoBaseUpload = None
    Request = None
    Credentials = None
    InstalledAppFlow = None


DEFAULT_SCOPES = ["https://www.googleapis.com/auth/drive.file"]
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


class GoogleClientUnavailable(RuntimeError):
    """Raised when Google client libraries are not available."""


def _ensure_google_client() -> None:
    if build is None or Credentials is None or InstalledAppFlow is None:
        raise GoogleClientUnavailable(
            "Google API client libraries are required. Install 'google-api-python-client' "
            "and 'google-auth-oauthlib'."
        )


def init_client(
    secret_path: str,
    token_path: str,
    scopes: Optional[List[str]] = None,
):
    """Initialise a Drive API client using OAuth desktop flow."""

    _ensure_google_client()

    scopes = scopes or DEFAULT_SCOPES
    if not os.path.exists(secret_path):
        raise FileNotFoundError(f"Client secret file not found: {secret_path}")

    credentials = None
    if token_path and os.path.exists(token_path):
        credentials = Credentials.from_authorized_user_file(token_path, scopes)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(secret_path, scopes)
            credentials = flow.run_local_server(port=0)
        if token_path:
            os.makedirs(os.path.dirname(token_path) or ".", exist_ok=True)
            with open(token_path, "w", encoding="utf-8") as handle:
                handle.write(credentials.to_json())

    return build("drive", "v3", credentials=credentials)


def _format_rfc3339(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def ensure_folder(service, name: str, parent_id: Optional[str] = None) -> str:
    """Ensure that a folder with the given name exists and return its ID."""

    parent_ref = parent_id or "root"
    escaped_name = name.replace("'", "\\'")
    query = (
        " and ".join(
            [
                f"mimeType = '{FOLDER_MIME_TYPE}'",
                "trashed = false",
                f"name = '{escaped_name}'",
                f"'{parent_ref}' in parents",
            ]
        )
    )
    response = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id, name)")
        .execute()
    )
    files = response.get("files", [])
    if files:
        return files[0]["id"]

    metadata = {
        "name": name,
        "mimeType": FOLDER_MIME_TYPE,
        "parents": [parent_ref],
    }
    created = service.files().create(body=metadata, fields="id").execute()
    return created["id"]


def ensure_structure(
    service,
    root_folder_id: Optional[str] = None,
    root_name: str = "RugBaseSync",
) -> Dict[str, str]:
    """Ensure the expected folder hierarchy exists and return folder IDs."""

    root_id = root_folder_id
    if root_id:
        try:
            service.files().get(fileId=root_id, fields="id").execute()
        except HttpError:
            root_id = None
    if not root_id:
        root_id = ensure_folder(service, root_name)

    changelog_id = ensure_folder(service, "changelog", parent_id=root_id)
    backups_id = ensure_folder(service, "backups", parent_id=root_id)
    return {"root": root_id, "changelog": changelog_id, "backups": backups_id}


def upload_json(service, parent_id: str, filename: str, data: Dict) -> Dict:
    """Upload a JSON file containing change data to Drive."""

    _ensure_google_client()
    media = MediaIoBaseUpload(
        io.BytesIO(json.dumps(data, ensure_ascii=False).encode("utf-8")),
        mimetype="application/json",
        resumable=False,
    )
    metadata = {"name": filename, "parents": [parent_id]}
    return (
        service.files()
        .create(body=metadata, media_body=media, fields="id, name")
        .execute()
    )


def upload_file(
    service,
    parent_id: str,
    filename: str,
    local_path: str,
    mime_type: str,
) -> Dict:
    """Upload a binary file such as a ZIP backup to Drive."""

    _ensure_google_client()
    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=False)
    metadata = {"name": filename, "parents": [parent_id]}
    return (
        service.files()
        .create(body=metadata, media_body=media, fields="id, name")
        .execute()
    )


def list_files(service, parent_id: str, since_datetime: Optional[datetime] = None) -> List[Dict]:
    """List files within a parent folder optionally filtered by modified time."""

    query_parts = [f"'{parent_id}' in parents", "trashed = false"]
    if since_datetime is not None:
        query_parts.append(f"modifiedTime > '{_format_rfc3339(since_datetime)}'")
    query = " and ".join(query_parts)

    files: List[Dict] = []
    page_token: Optional[str] = None
    while True:
        response = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, createdTime, modifiedTime)",
                orderBy="createdTime",
                pageToken=page_token,
            )
            .execute()
        )
        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    files.sort(key=lambda item: item.get("createdTime", ""))
    return files


def download_file(service, file_id: str) -> bytes:
    """Download a file's content as bytes."""

    _ensure_google_client()
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    from googleapiclient.http import MediaIoBaseDownload  # Imported lazily to avoid optional dependency issues

    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buffer.getvalue()
