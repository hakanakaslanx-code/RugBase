"""Runtime helpers for loading optional third-party dependencies.

This module is executed at application start-up to ensure that optional
dependencies—primarily the Google API client libraries—are importable.  It
supports the frozen PyInstaller build by detecting a sibling ``vendor``
directory that can contain wheel extractions of the required packages.

If the libraries are unavailable we record a detailed, user-facing message so
that the rest of the application can gracefully disable Google Drive/Sheets
features instead of terminating.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Iterable, Optional

from core import deps_bootstrap

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PyInstaller integration
# ---------------------------------------------------------------------------

# NOTE: keep this list in sync with ``build_exe.py``.
HIDDEN_IMPORTS: Iterable[str] = (
    "googleapiclient",
    "googleapiclient.discovery",
    "googleapiclient.http",
    "google_auth_oauthlib.flow",
    "google.oauth2.service_account",
    "httplib2",
)

_MISSING_DEPENDENCY_MESSAGE = (
    "Google kütüphaneleri bulunamadı. Lütfen build’e hiddenimports ekleyin "
    "ya da EXE ile aynı klasöre ‘vendor’ klasörünü koyun."
)

_runtime_root: Optional[Path] = None
_vendor_path: Optional[Path] = None
_google_available = False
_missing_message = _MISSING_DEPENDENCY_MESSAGE
_initialised = False


def _detect_runtime_root() -> Path:
    if getattr(sys, "frozen", False):  # PyInstaller executable
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _ensure_vendor_on_path(root: Path) -> Optional[Path]:
    vendor = root / "vendor"
    if not vendor.exists() or not vendor.is_dir():
        logger.debug("Vendor directory not found at %s", vendor)
        return None

    vendor_str = str(vendor)
    if vendor_str not in sys.path:
        sys.path.insert(0, vendor_str)
    try:
        import site

        site.addsitedir(vendor_str)
    except Exception:  # pragma: no cover - defensive guard
        logger.debug("Failed to register vendor directory %s", vendor_str, exc_info=True)
    return vendor


def bootstrap() -> bool:
    """Initialise dependency loading for the current process."""

    global _runtime_root, _vendor_path, _google_available, _initialised

    if _initialised:
        return _google_available

    _runtime_root = _detect_runtime_root()
    os.environ.setdefault("RUGBASE_RUNTIME_ROOT", str(_runtime_root))
    _vendor_path = _ensure_vendor_on_path(_runtime_root)
    if _vendor_path:
        os.environ["RUGBASE_VENDOR_PATH"] = str(_vendor_path)

    _google_available = deps_bootstrap.ensure_google_deps()
    if not _google_available:
        os.environ.setdefault("RUGBASE_DEPENDENCY_WARNING", _MISSING_DEPENDENCY_MESSAGE)
        logger.warning(_MISSING_DEPENDENCY_MESSAGE)
    else:
        os.environ.pop("RUGBASE_DEPENDENCY_WARNING", None)

    _initialised = True
    return _google_available


def google_dependencies_available() -> bool:
    """Return ``True`` if Google client libraries are importable."""

    if not _initialised:
        return bootstrap()
    return _google_available


def dependency_warning() -> str:
    """Return the human-readable warning for missing Google dependencies."""

    return os.environ.get("RUGBASE_DEPENDENCY_WARNING", _missing_message)


def runtime_root() -> Path:
    """Return the detected application runtime root."""

    if not _initialised:
        bootstrap()
    assert _runtime_root is not None  # for type-checkers
    return _runtime_root


def vendor_path() -> Optional[Path]:
    """Return the detected vendor directory, if any."""

    if not _initialised:
        bootstrap()
    return _vendor_path


def default_credentials_path(filename: str = "credentials.json") -> Optional[Path]:
    """Return the default credentials file bundled with the application."""

    root = runtime_root()
    candidate = root / "credentials" / filename
    return candidate if candidate.exists() else None


__all__ = [
    "HIDDEN_IMPORTS",
    "bootstrap",
    "google_dependencies_available",
    "dependency_warning",
    "runtime_root",
    "vendor_path",
    "default_credentials_path",
]

