"""Automatic update helpers for RugBase."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

try:  # pragma: no cover - Tkinter may not be available in headless tests
    from tkinter import messagebox  # type: ignore
except Exception:  # pragma: no cover - graceful fallback when Tk is unavailable
    messagebox = None  # type: ignore

from .version import __version__

CONFIG_FILENAME = "update_config.json"
DEFAULT_CONFIG: Dict[str, str] = {
    "version_url": "rugbase.txt",
    "download_url": "https://example.com/downloads/RugBase.exe",
    "download_filename": "RugBase.exe",
    "changelog_url": "",
}


class UpdateError(RuntimeError):
    """Base class for update related errors."""


class UpdateConfigurationError(UpdateError):
    """Raised when the update subsystem is misconfigured."""


@dataclass
class UpdateInfo:
    """Details about the local and remote RugBase versions."""

    local_version: str
    remote_version: str
    download_url: str
    download_filename: str
    changelog_url: Optional[str] = None

    @property
    def update_available(self) -> bool:
        return _is_remote_newer(self.remote_version, self.local_version)


def load_config() -> Dict[str, str]:
    """Load update configuration from disk or fall back to defaults."""

    override = os.environ.get("RUGBASE_UPDATE_CONFIG")
    if override:
        config_path = pathlib.Path(override).expanduser()
    else:
        config_path = _config_path()

    config: Dict[str, str] = dict(DEFAULT_CONFIG)
    if config_path.exists():
        try:
            loaded = json.loads(config_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:  # pragma: no cover - configuration error
            raise UpdateConfigurationError(f"Invalid JSON in {config_path}: {exc}") from exc
        if not isinstance(loaded, dict):  # pragma: no cover - configuration error
            raise UpdateConfigurationError(f"Update configuration in {config_path} must be an object")
        config.update({str(key): str(value) for key, value in loaded.items()})

    return config


def check_for_update() -> UpdateInfo:
    """Return information about the latest available version."""

    config = _validate_config(load_config())

    remote_version = _read_version(config["version_url"])
    if not remote_version:
        raise UpdateError("Received an empty version string from the update source")

    download_filename = config.get("download_filename") or _infer_filename(config["download_url"])

    return UpdateInfo(
        local_version=__version__,
        remote_version=remote_version,
        download_url=config["download_url"],
        download_filename=download_filename,
        changelog_url=config.get("changelog_url") or None,
    )


def download_update(info: UpdateInfo, *, inplace: bool = False) -> pathlib.Path:
    """Download the latest release and return the filesystem path."""

    _ensure_download_url(info.download_url)

    destination_dir = _install_directory() if inplace else _updates_directory()
    destination_dir.mkdir(parents=True, exist_ok=True)

    target_name = info.download_filename
    if not inplace:
        stem = pathlib.Path(info.download_filename).stem
        suffix = pathlib.Path(info.download_filename).suffix
        safe_version = _safe_version_tag(info.remote_version)
        target_name = f"{stem}-{safe_version}{suffix}" if suffix else f"{stem}-{safe_version}"

    destination = destination_dir / target_name

    if inplace and destination.exists():
        backup = _next_backup_name(destination)
        destination.replace(backup)

    _download_to_path(info.download_url, destination)
    return destination


def prompt_for_update(parent: Optional[object] = None) -> None:
    """Display interactive update prompts for the running application."""

    try:
        info = check_for_update()
    except UpdateError as exc:
        _show_message("Check for Updates", str(exc), error=True, parent=parent)
        return

    if not info.update_available:
        _show_message(
            "Check for Updates",
            f"You are already running the latest version ({info.local_version}).",
            parent=parent,
        )
        return

    message_lines = [
        f"Current version: {info.local_version}",
        f"Latest version: {info.remote_version}",
        "",
        "Would you like to download the update now?",
    ]
    if info.changelog_url:
        message_lines.insert(3, f"Release notes: {info.changelog_url}")

    prompt = "\n".join(message_lines)

    if messagebox is None:
        print("Update available:\n" + prompt)
        try:
            destination = download_update(info)
        except UpdateError as exc:  # pragma: no cover - runtime error path
            print(f"Failed to download update: {exc}", file=sys.stderr)
            return
        print(f"Update downloaded to: {destination}")
        return

    if not messagebox.askyesno("Update Available", prompt, parent=parent):
        return

    try:
        destination = download_update(info)
    except UpdateError as exc:
        _show_message("Check for Updates", f"Failed to download update: {exc}", error=True, parent=parent)
        return

    instructions = (
        "Download complete. Close RugBase and run 'update.bat' to install the new version.\n"
        f"Saved to: {destination}"
    )
    _show_message("Update Downloaded", instructions, parent=parent)


def main(argv: Optional[list[str]] = None) -> int:
    """Entry point for command-line usage."""

    parser = argparse.ArgumentParser(description="RugBase updater utility")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check for a newer version and report the result",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download the newest version to the updates folder",
    )
    parser.add_argument(
        "--batch-update",
        action="store_true",
        help="Download and replace the existing executable (requires RugBase to be closed)",
    )

    args = parser.parse_args(argv)

    try:
        info = check_for_update()
    except UpdateError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.check or not any((args.download, args.batch_update)):
        if info.update_available:
            print(
                "New version available!\n"
                f"Current version: {info.local_version}\n"
                f"Latest version: {info.remote_version}"
            )
            if info.changelog_url:
                print(f"Release notes: {info.changelog_url}")
        else:
            print(f"RugBase is up to date (version {info.local_version}).")
        return 0

    if args.download:
        if not info.update_available:
            print("Already running the latest version. No download required.")
            return 0
        try:
            destination = download_update(info)
        except UpdateError as exc:
            print(f"Download failed: {exc}", file=sys.stderr)
            return 1
        print(f"Update downloaded to: {destination}")
        return 0

    if args.batch_update:
        if not info.update_available:
            print("RugBase is already up to date.")
            return 0
        try:
            destination = download_update(info, inplace=True)
        except UpdateError as exc:
            print(f"Automatic update failed: {exc}", file=sys.stderr)
            return 1
        print(f"Update installed to: {destination}")
        return 0

    return 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _project_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def _config_path() -> pathlib.Path:
    return pathlib.Path(__file__).with_name(CONFIG_FILENAME)


def _install_directory() -> pathlib.Path:
    if getattr(sys, "frozen", False):  # Running inside PyInstaller bundle
        return pathlib.Path(sys.executable).resolve().parent
    return _project_root()


def _updates_directory() -> pathlib.Path:
    return _install_directory() / "updates"


def _validate_config(config: Dict[str, str]) -> Dict[str, str]:
    version_url = config.get("version_url", "").strip()
    download_url = config.get("download_url", "").strip()

    if not version_url:
        raise UpdateConfigurationError(
            "Missing 'version_url' in update configuration. Edit core/update_config.json."
        )

    config["version_url"] = version_url
    config["download_url"] = download_url
    return config


def _ensure_download_url(url: str) -> None:
    if not url:
        raise UpdateConfigurationError(
            "Update download URL is not configured. Edit core/update_config.json with your release location."
        )
    placeholders = ("example.com", "your-account", "your-org", "YOUR_")
    if any(token in url for token in placeholders):
        raise UpdateConfigurationError(
            "Update download URL still uses a placeholder value. Update core/update_config.json."
        )


def _resolve_source(path_or_url: str) -> Tuple[str, bool]:
    if "//" in path_or_url:
        return path_or_url, True
    absolute = _project_root() / path_or_url
    return str(absolute), False


def _read_version(source: str) -> str:
    location, remote = _resolve_source(source)
    if remote:
        try:
            with urllib.request.urlopen(location, timeout=10) as response:
                data = response.read().decode("utf-8")
        except urllib.error.URLError as exc:
            raise UpdateError(f"Failed to fetch version information: {exc}") from exc
    else:
        try:
            data = pathlib.Path(location).read_text(encoding="utf-8")
        except OSError as exc:
            raise UpdateError(f"Failed to read version file: {exc}") from exc
    return data.strip()


def _infer_filename(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    name = pathlib.Path(parsed.path).name
    return name or "RugBase.exe"


def _is_remote_newer(remote: str, local: str) -> bool:
    return _version_key(remote) > _version_key(local)


def _version_key(value: str) -> Tuple[int | str, ...]:
    separators = ".-_/"
    normalized = value
    for separator in separators[1:]:
        normalized = normalized.replace(separator, separators[0])
    parts = [part for part in normalized.split(separators[0]) if part]
    key: list[int | str] = []
    for part in parts:
        if part.isdigit():
            key.append(int(part))
        else:
            key.append(part.lower())
    return tuple(key)


def _safe_version_tag(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_", "."} else "-" for char in value)


def _next_backup_name(path: pathlib.Path) -> pathlib.Path:
    timestamp = int(time.time())
    candidate = path.with_suffix(path.suffix + f".bak-{timestamp}")
    counter = 1
    while candidate.exists():
        candidate = path.with_suffix(path.suffix + f".bak-{timestamp}-{counter}")
        counter += 1
    return candidate


def _download_to_path(source: str, destination: pathlib.Path) -> None:
    location, remote = _resolve_source(source)
    destination.parent.mkdir(parents=True, exist_ok=True)

    if remote:
        tmp_file = tempfile.NamedTemporaryFile(delete=False, dir=str(destination.parent))
        try:
            with urllib.request.urlopen(location, timeout=60) as response, open(tmp_file.name, "wb") as tmp_handle:
                shutil.copyfileobj(response, tmp_handle)
            pathlib.Path(tmp_file.name).replace(destination)
        except urllib.error.URLError as exc:
            pathlib.Path(tmp_file.name).unlink(missing_ok=True)
            raise UpdateError(f"Failed to download update: {exc}") from exc
        except OSError as exc:
            pathlib.Path(tmp_file.name).unlink(missing_ok=True)
            raise UpdateError(f"Failed to save update: {exc}") from exc
    else:
        try:
            shutil.copyfile(location, destination)
        except OSError as exc:
            raise UpdateError(f"Failed to copy update from {location}: {exc}") from exc


def _show_message(title: str, message: str, *, error: bool = False, parent: Optional[object] = None) -> None:
    if messagebox is None:
        stream = sys.stderr if error else sys.stdout
        print(f"{title}: {message}", file=stream)
        return

    if error:
        messagebox.showerror(title, message, parent=parent)
    else:
        messagebox.showinfo(title, message, parent=parent)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
