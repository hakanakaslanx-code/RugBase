"""Dependency checks for optional runtime integrations."""
from __future__ import annotations

import importlib
import logging
from typing import List, Sequence

logger = logging.getLogger(__name__)

GOOGLE_IMPORTS: Sequence[str] = (
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

PILLOW_IMPORTS: Sequence[str] = (
    "PIL",
    "PIL.Image",
    "PIL.ImageFont",
    "PIL.ImageDraw",
    "PIL._imaging",
)

_missing_google_imports: List[str] = []
_missing_pillow_imports: List[str] = []


def _try_import(module_name: str) -> bool:
    try:
        importlib.import_module(module_name)
    except (ImportError, FileNotFoundError) as exc:  # pragma: no cover - depends on environment
        logger.debug("[Deps] import error for %s: %s", module_name, exc, exc_info=True)
        return False
    return True


def _check_imports(module_names: Sequence[str]) -> List[str]:
    missing: List[str] = []
    for module_name in module_names:
        if not _try_import(module_name):
            missing.append(module_name)
    return missing


def check_google_deps() -> List[str]:
    """Return the list of missing Google client modules."""

    return _check_imports(GOOGLE_IMPORTS)


def check_pillow_deps() -> List[str]:
    """Return the list of missing Pillow modules."""

    return _check_imports(PILLOW_IMPORTS)


def ensure_google_deps(force_reinstall: bool = False) -> bool:
    """Verify that the required Google modules are importable."""

    del force_reinstall  # runtime installs are no longer supported
    global _missing_google_imports
    _missing_google_imports = check_google_deps()
    if _missing_google_imports:
        logger.warning(
            "[Deps] Sync module dependencies missing: %s",
            ", ".join(_missing_google_imports),
        )
        return False
    logger.info("[Deps] Google sync dependencies ready.")
    return True


def missing_dependencies() -> Sequence[str]:
    """Return the last computed list of missing Google modules."""

    return tuple(_missing_google_imports)


def ensure_pillow_deps() -> bool:
    """Verify that the Pillow modules are importable."""

    global _missing_pillow_imports
    _missing_pillow_imports = check_pillow_deps()
    if _missing_pillow_imports:
        logger.warning(
            "[Deps] Pillow dependencies missing: %s",
            ", ".join(_missing_pillow_imports),
        )
        return False
    logger.info("[Deps] Pillow dependencies ready.")
    return True


def missing_pillow_dependencies() -> Sequence[str]:
    """Return the last computed list of missing Pillow modules."""

    return tuple(_missing_pillow_imports)


__all__ = [
    "GOOGLE_IMPORTS",
    "PILLOW_IMPORTS",
    "check_google_deps",
    "check_pillow_deps",
    "ensure_google_deps",
    "ensure_pillow_deps",
    "missing_dependencies",
    "missing_pillow_dependencies",
]
