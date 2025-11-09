"""Centralised helpers for managing RugBase application directories."""
from __future__ import annotations

import logging
import os
import site
import sys
from pathlib import Path
from typing import Iterable

from dependency_loader import runtime_root

logger = logging.getLogger(__name__)

_APP_ENV_VARS: Iterable[str] = ("LOCALAPPDATA", "APPDATA")


def _detect_base_directory() -> Path:
    for env_var in _APP_ENV_VARS:
        value = os.environ.get(env_var)
        if value:
            return Path(value).expanduser().resolve() / "RugBase"
    return Path.home().resolve() / ".rugbase"


APP_DIR: Path = _detect_base_directory()
CONFIG_DIR: Path = APP_DIR / "config"
CREDENTIALS_DIR: Path = APP_DIR / "credentials"
TOKENS_DIR: Path = APP_DIR
VENDOR_DIR: Path = APP_DIR / "vendor"
BACKUP_DIR: Path = APP_DIR / "backups"
LOG_DIR: Path = APP_DIR / "logs"
DEPENDENCY_DIR: Path = APP_DIR / "pydeps"


def install_path(*parts: str) -> Path:
    """Return a path relative to the application installation root."""

    root = runtime_root()
    return root.joinpath(*parts)


def ensure_directory(path: Path) -> Path:
    """Ensure that ``path`` exists, returning the :class:`~pathlib.Path`."""

    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_app_structure() -> None:
    """Create the base directories required for application data."""

    for directory in (
        APP_DIR,
        CONFIG_DIR,
        CREDENTIALS_DIR,
        BACKUP_DIR,
        LOG_DIR,
        DEPENDENCY_DIR,
    ):
        ensure_directory(directory)


def data_path(*parts: str) -> Path:
    """Return a path rooted inside :data:`APP_DIR`, creating parent directories."""

    ensure_app_structure()
    target = APP_DIR.joinpath(*parts)
    if target.parent and not target.parent.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
    return target


def config_path(*parts: str) -> Path:
    """Return a path inside the configuration directory."""

    ensure_directory(CONFIG_DIR)
    target = CONFIG_DIR.joinpath(*parts)
    if target.parent and not target.parent.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
    return target


def tokens_path(*parts: str) -> Path:
    """Return a path inside the managed tokens directory."""

    ensure_directory(TOKENS_DIR)
    target = TOKENS_DIR.joinpath(*parts)
    if target.parent and not target.parent.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
    return target


def credentials_path(*parts: str) -> Path:
    """Return a path inside the credentials directory."""

    ensure_directory(CREDENTIALS_DIR)
    target = CREDENTIALS_DIR.joinpath(*parts)
    if target.parent and not target.parent.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
    return target


def logs_path(*parts: str) -> Path:
    """Return a path inside the application log directory."""

    ensure_directory(LOG_DIR)
    target = LOG_DIR.joinpath(*parts)
    if target.parent and not target.parent.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
    return target


def dependencies_path(*parts: str) -> Path:
    """Return a path inside the managed dependency directory."""

    ensure_directory(DEPENDENCY_DIR)
    target = DEPENDENCY_DIR.joinpath(*parts)
    if target.parent and not target.parent.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
    return target


def ensure_vendor_on_path() -> Path:
    """Ensure the vendored dependency directory is on ``sys.path``."""

    ensure_directory(VENDOR_DIR)
    vendor_str = str(VENDOR_DIR)
    if vendor_str not in sys.path:
        sys.path.insert(0, vendor_str)
    try:
        site.addsitedir(vendor_str)
    except Exception:  # pragma: no cover - defensive guard
        logger.debug("Failed to register vendor directory %s", vendor_str, exc_info=True)
    return VENDOR_DIR


__all__ = [
    "APP_DIR",
    "CONFIG_DIR",
    "TOKENS_DIR",
    "CREDENTIALS_DIR",
    "VENDOR_DIR",
    "BACKUP_DIR",
    "LOG_DIR",
    "DEPENDENCY_DIR",
    "install_path",
    "data_path",
    "config_path",
    "tokens_path",
    "credentials_path",
    "logs_path",
    "dependencies_path",
    "ensure_app_structure",
    "ensure_directory",
    "ensure_vendor_on_path",
]
