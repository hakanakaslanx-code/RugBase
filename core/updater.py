"""Automatic update utilities for RugBase.

This module implements the fully automated update workflow described in the
project requirements.  It communicates with GitHub releases, downloads the
latest ``.zip`` asset, prepares a hidden batch updater, and restarts the
application once the update has been applied.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Optional

from .version import __version__

GITHUB_REPO = "hakanakaslanx-code/RugBase"
LATEST_RELEASE_URL = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
USER_AGENT = "RugBase-Updater"


class UpdateError(RuntimeError):
    """Raised when the updater cannot complete an operation."""


@dataclass
class ReleaseInfo:
    """Information about a GitHub release asset."""

    tag: str
    asset_name: str
    asset_url: str

    @property
    def normalized_version(self) -> str:
        return _strip_version_prefix(self.tag)

    @property
    def is_archive(self) -> bool:
        return self.asset_name.lower().endswith(".zip")


@dataclass
class UpdateStatus:
    """Represents the local and remote versions."""

    local_version: str
    remote_version: str
    release: Optional[ReleaseInfo]

    @property
    def update_available(self) -> bool:
        if not self.remote_version or not self.release:
            return False
        return _is_remote_newer(self.remote_version, self.local_version)


# -- Public API -------------------------------------------------------------

def get_update_status() -> UpdateStatus:
    """Return information about the latest release on GitHub."""

    try:
        release = _fetch_latest_release()
    except UpdateError:
        raise
    except Exception as exc:  # pragma: no cover - network or JSON parsing issues
        raise UpdateError(f"Unable to query GitHub releases: {exc}") from exc

    remote_version = release.normalized_version if release else ""
    return UpdateStatus(local_version=__version__, remote_version=remote_version, release=release)


def download_release_asset(release: ReleaseInfo) -> pathlib.Path:
    """Download the given release asset to the user's temporary directory."""

    if os.name != "nt":  # pragma: no cover - updater is Windows specific
        raise UpdateError("The RugBase updater is only supported on Windows.")

    download_dir = pathlib.Path(tempfile.mkdtemp(prefix="RugBase_Update_"))
    destination = download_dir / release.asset_name

    request = urllib.request.Request(
        release.asset_url,
        headers={"User-Agent": USER_AGENT},
    )

    try:
        with urllib.request.urlopen(request) as response:  # nosec: B310 - trusted URL from release
            data = response.read()
    except urllib.error.URLError as exc:  # pragma: no cover - network error path
        raise UpdateError(f"Unable to download the update: {exc}") from exc

    try:
        destination.write_bytes(data)
    except OSError as exc:
        raise UpdateError(f"Failed to save the downloaded update: {exc}") from exc

    return destination


def prepare_updater(asset_path: pathlib.Path, release: ReleaseInfo) -> pathlib.Path:
    """Create the RugBase_Updater.bat script for the downloaded asset."""

    if os.name != "nt":  # pragma: no cover - updater is Windows specific
        raise UpdateError("The RugBase updater is only supported on Windows.")

    exe_path = _current_executable()
    install_dir = exe_path.parent
    version_tag = _sanitize_for_filename(release.normalized_version)
    timestamp = time.strftime("%Y%m%d%H%M%S")
    backup_name = f"{exe_path.stem}_backup_{version_tag}_{timestamp}{exe_path.suffix or '.bak'}"

    updater_dir = asset_path.parent
    script_path = updater_dir / "RugBase_Updater.bat"

    script_contents = _build_updater_script(
        asset_path=asset_path,
        install_dir=install_dir,
        exe_name=exe_path.name,
        backup_name=backup_name,
        version_label=release.normalized_version,
        is_archive=release.is_archive,
    )

    try:
        script_path.write_text(script_contents, encoding="utf-8")
    except OSError as exc:
        raise UpdateError(f"Failed to create updater script: {exc}") from exc

    return script_path


def launch_updater(script_path: pathlib.Path) -> None:
    """Launch the updater script in a hidden background process."""

    if os.name != "nt":  # pragma: no cover - updater is Windows specific
        raise UpdateError("The RugBase updater is only supported on Windows.")

    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

    try:
        subprocess.Popen(
            ["cmd.exe", "/c", str(script_path)],
            creationflags=creationflags,
            close_fds=True,
        )
    except OSError as exc:  # pragma: no cover - process creation failure
        raise UpdateError(f"Unable to start the updater script: {exc}") from exc


def check_for_updates(parent: Optional[object] = None) -> bool:
    """Check GitHub for a new release and schedule the update if available.

    The function displays message boxes when a GUI parent is supplied.  It
    returns ``True`` when an update has been scheduled and ``False`` otherwise.
    """

    try:
        status = get_update_status()
    except UpdateError as exc:
        _notify_user("Check for Updates", str(exc), parent, error=True)
        return False

    if not status.update_available or not status.release:
        _notify_user("Check for Updates", "Already up to date.", parent)
        return False

    if not _confirm_update(status, parent):
        return False

    try:
        asset_path = download_release_asset(status.release)
    except UpdateError as exc:
        _notify_user("Check for Updates", str(exc), parent, error=True)
        return False

    try:
        script_path = prepare_updater(asset_path, status.release)
    except UpdateError as exc:
        _notify_user("Check for Updates", str(exc), parent, error=True)
        return False

    try:
        launch_updater(script_path)
    except UpdateError as exc:
        _notify_user("Check for Updates", str(exc), parent, error=True)
        return False

    _notify_user(
        "Check for Updates",
        "Update downloaded. RugBase will close and restart to install the update.",
        parent,
    )
    _request_application_restart(parent)
    return True


# -- Internal helpers ------------------------------------------------------

def _fetch_latest_release() -> Optional[ReleaseInfo]:
    request = urllib.request.Request(
        LATEST_RELEASE_URL,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": USER_AGENT,
        },
    )

    try:
        with urllib.request.urlopen(request) as response:  # nosec: B310 - GitHub API
            payload = response.read()
    except urllib.error.URLError as exc:
        raise UpdateError(f"Unable to contact GitHub: {exc}") from exc

    try:
        data = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UpdateError(f"Unexpected response from GitHub: {exc}") from exc

    tag = str(data.get("tag_name") or "").strip()
    assets = data.get("assets") or []

    asset_url = ""
    asset_name = ""
    fallback_url = ""
    fallback_name = ""
    for asset in assets:
        name = str(asset.get("name") or "")
        download_url = str(asset.get("browser_download_url") or "")
        if not download_url:
            continue
        if name.lower().endswith(".zip"):
            asset_name = name
            asset_url = download_url
            break
        if not fallback_url:
            fallback_name = name or f"RugBase-{tag}"
            fallback_url = download_url

    if not tag:
        raise UpdateError("Latest release on GitHub does not have a tag name.")

    if not asset_url:
        if fallback_url:
            asset_name = fallback_name
            asset_url = fallback_url
        else:
            raise UpdateError("Latest release does not provide a downloadable asset.")

    return ReleaseInfo(tag=tag, asset_name=asset_name or f"RugBase-{tag}.zip", asset_url=asset_url)


def _strip_version_prefix(version: str) -> str:
    version = version.strip()
    if version.lower().startswith("v"):
        version = version[1:]
    return version


def _is_remote_newer(remote: str, local: str) -> bool:
    return _version_tuple(remote) > _version_tuple(local)


def _version_tuple(version: str) -> tuple[int, ...]:
    cleaned = _strip_version_prefix(version)
    parts: list[int] = []
    for segment in cleaned.replace("-", ".").split("."):
        digits = ""
        for char in segment:
            if char.isdigit():
                digits += char
            else:
                break
        if digits:
            parts.append(int(digits))
        else:
            parts.append(0)
    return tuple(parts or [0])


def _current_executable() -> pathlib.Path:
    if getattr(sys, "frozen", False):
        return pathlib.Path(sys.executable).resolve()

    script_path = pathlib.Path(sys.argv[0]).expanduser()
    if not script_path.is_absolute():
        script_path = pathlib.Path.cwd() / script_path

    resolved = script_path.resolve()
    if resolved.exists():
        return resolved

    base_dir = pathlib.Path(getattr(sys, "_MEIPASS", pathlib.Path(__file__).resolve().parent))
    fallback = (base_dir / script_path.name).resolve()
    return fallback


def _sanitize_for_filename(value: str) -> str:
    if not value:
        return "latest"
    safe_chars = [c if c.isalnum() else "_" for c in value]
    sanitized = "".join(safe_chars).strip("_")
    return sanitized or "latest"


def _build_updater_script(
    *,
    asset_path: pathlib.Path,
    install_dir: pathlib.Path,
    exe_name: str,
    backup_name: str,
    version_label: str,
    is_archive: bool,
) -> str:
    asset_path = asset_path.resolve()
    install_dir = install_dir.resolve()

    commands: list[str] = [
        "@echo off",
        "setlocal enableextensions enabledelayedexpansion",
        f'set "ASSET_FILE={asset_path}"',
        f'set "INSTALL_DIR={install_dir}"',
        f'set "EXE_NAME={exe_name}"',
        f'set "BACKUP_NAME={backup_name}"',
        f'set "UPDATE_VERSION={version_label}"',
    ]

    if is_archive:
        commands.extend(
            [
                'set "EXTRACT_DIR=%TEMP%\\RugBase_Update_%RANDOM%_%RANDOM%"',
                'if exist "%EXTRACT_DIR%" rd /s /q "%EXTRACT_DIR%"',
                'mkdir "%EXTRACT_DIR%" >nul 2>&1',
            ]
        )

    commands.extend(
        [
            ":wait_for_exit",
            'move /Y "%INSTALL_DIR%\\%EXE_NAME%" "%INSTALL_DIR%\\%BACKUP_NAME%" >nul 2>&1',
            'if exist "%INSTALL_DIR%\\%EXE_NAME%" (',
            '  timeout /t 1 /nobreak >nul',
            '  goto wait_for_exit',
            ")",
        ]
    )

    if is_archive:
        commands.extend(
            [
                'powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -LiteralPath ''%ASSET_FILE%'' -DestinationPath ''%EXTRACT_DIR%'' -Force" >nul 2>&1',
                "if errorlevel 1 goto restore_backup",
                'set "NEW_EXE="',
                'for /r "%EXTRACT_DIR%" %%F in (*.exe) do (',
                '  set "NEW_EXE=%%~fF"',
                '  goto found_exe',
                ")",
                "goto restore_backup",
                ":found_exe",
                'if not defined NEW_EXE goto restore_backup',
                'copy /Y "!NEW_EXE!" "%INSTALL_DIR%\\%EXE_NAME%" >nul 2>&1',
                "if errorlevel 1 goto restore_backup",
            ]
        )
    else:
        commands.extend(
            [
                'copy /Y "%ASSET_FILE%" "%INSTALL_DIR%\\%EXE_NAME%" >nul 2>&1',
                "if errorlevel 1 goto restore_backup",
            ]
        )

    commands.extend(
        [
            'start "" "%INSTALL_DIR%\\%EXE_NAME%"',
            "goto cleanup",
            ":restore_backup",
            'if exist "%INSTALL_DIR%\\%BACKUP_NAME%" move /Y "%INSTALL_DIR%\\%BACKUP_NAME%" "%INSTALL_DIR%\\%EXE_NAME%" >nul 2>&1',
            ":cleanup",
        ]
    )

    if is_archive:
        commands.append('if exist "%EXTRACT_DIR%" rd /s /q "%EXTRACT_DIR%"')
    commands.append('if exist "%ASSET_FILE%" del "%ASSET_FILE%"')
    commands.append('if exist "%INSTALL_DIR%\\%BACKUP_NAME%" del "%INSTALL_DIR%\\%BACKUP_NAME%"')
    commands.append("endlocal")
    commands.append('del "%~f0"')
    commands.append("")

    return "\r\n".join(commands)


def _notify_user(title: str, message: str, parent: Optional[object], *, error: bool = False) -> None:
    try:
        from tkinter import messagebox  # type: ignore
    except Exception:  # pragma: no cover - tkinter may be unavailable
        messagebox = None  # type: ignore

    if messagebox and parent is not None:
        if error:
            messagebox.showerror(title, message, parent=parent)  # type: ignore[arg-type]
        else:
            messagebox.showinfo(title, message, parent=parent)  # type: ignore[arg-type]
    elif messagebox:
        if error:
            messagebox.showerror(title, message)  # type: ignore[arg-type]
        else:
            messagebox.showinfo(title, message)  # type: ignore[arg-type]
    else:  # pragma: no cover - console fallback
        output = f"{title}: {message}"
        if error:
            print(output, file=sys.stderr)
        else:
            print(output)


def _confirm_update(status: UpdateStatus, parent: Optional[object]) -> bool:
    message = (
        "A new version of RugBase is available.\n"
        f"Current version: {status.local_version}\n"
        f"Latest version: {status.remote_version}\n\n"
        "Would you like to download and install it now?"
    )

    try:
        from tkinter import messagebox  # type: ignore
    except Exception:  # pragma: no cover - tkinter may be unavailable
        messagebox = None  # type: ignore

    if messagebox:
        if parent is not None:
            return bool(messagebox.askyesno("Check for Updates", message, parent=parent))  # type: ignore[arg-type]
        return bool(messagebox.askyesno("Check for Updates", message))  # type: ignore[arg-type]

    response = input(f"{message}\nType 'y' to continue: ")  # pragma: no cover - console fallback
    return response.strip().lower() in {"y", "yes"}


def _request_application_restart(parent: Optional[object]) -> None:
    if parent is not None:
        try:
            parent.after(250, getattr(parent, "quit", parent.destroy))
            parent.after(750, getattr(parent, "destroy", lambda: None))
        except Exception:  # pragma: no cover - defensive programming
            pass

    def _force_exit() -> None:
        time.sleep(1.5)
        os._exit(0)

    threading.Thread(target=_force_exit, name="RugBaseUpdaterExit", daemon=True).start()


__all__ = [
    "ReleaseInfo",
    "UpdateStatus",
    "UpdateError",
    "get_update_status",
    "download_release_asset",
    "prepare_updater",
    "launch_updater",
    "check_for_updates",
]
