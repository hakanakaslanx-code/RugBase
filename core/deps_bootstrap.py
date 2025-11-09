"""Dependency checks for Google Drive/Sheets integrations."""
from __future__ import annotations

import importlib
import logging
from typing import List, Sequence

logger = logging.getLogger(__name__)

REQUIRED_IMPORTS: Sequence[str] = (
    "googleapiclient.discovery",
    "googleapiclient.http",
    "googleapiclient._auth",
    "googleapiclient._helpers",
    "google.oauth2.service_account",
    "google.auth.transport.requests",
    "google_auth_oauthlib.flow",
    "httplib2",
    "oauthlib.oauth2",
)

_missing_imports: List[str] = []


def _try_import(module_name: str) -> bool:
    try:
        importlib.import_module(module_name)
    except (ImportError, FileNotFoundError) as exc:  # pragma: no cover - depends on environment
        logger.debug("[Deps] import error for %s: %s", module_name, exc, exc_info=True)
        return False
    return True


def check_google_deps() -> List[str]:
    """Return the list of missing Google client modules."""

    missing: List[str] = []
    for module_name in REQUIRED_IMPORTS:
        if not _try_import(module_name):
            missing.append(module_name)
    return missing


def ensure_google_deps(force_reinstall: bool = False) -> bool:
    """Verify that the required Google modules are importable."""

    del force_reinstall  # runtime installs are no longer supported
    global _missing_imports
    _missing_imports = check_google_deps()
    if _missing_imports:
        logger.warning(
            "[Deps] Senkron modülü bağımlılıkları eksik: %s",
            ", ".join(_missing_imports),
        )
        return False
    logger.info("[Deps] Google senkronizasyon bağımlılıkları hazır.")
    return True


def missing_dependencies() -> Sequence[str]:
    """Return the last computed list of missing Google modules."""

    return tuple(_missing_imports)


__all__ = ["check_google_deps", "ensure_google_deps", "missing_dependencies", "REQUIRED_IMPORTS"]
