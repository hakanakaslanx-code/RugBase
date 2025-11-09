"""Minimal helpers for verifying optional runtime dependencies."""
from __future__ import annotations

import importlib
import importlib.util
import logging
import sys
from pathlib import Path
from typing import Iterable, List, Sequence

from core import app_paths

logger = logging.getLogger(__name__)


class DependencyManager:
    """Utility helpers for dependency path management and verification."""

    install_target: Path = app_paths.DEPENDENCY_DIR
    log_directory: Path = app_paths.LOG_DIR

    @classmethod
    def ensure_paths(cls) -> None:
        """Ensure that dependency and log directories exist."""

        app_paths.ensure_directory(cls.install_target)
        app_paths.ensure_directory(cls.log_directory)
        logger.debug("[Deps] dependency directory prepared at %s", cls.install_target)

    @classmethod
    def add_to_sys_path(cls) -> bool:
        """Add the managed dependency directory to ``sys.path`` if present."""

        cls.ensure_paths()
        target = str(cls.install_target)
        if not cls.install_target.exists():
            logger.debug("[Deps] dependency directory missing: %s", target)
            return False
        if target not in sys.path:
            sys.path.insert(0, target)
            logger.debug("[Deps] dependency directory appended to sys.path: %s", target)
            return True
        logger.debug("[Deps] dependency directory already available on sys.path")
        return True

    @staticmethod
    def verify_imports(modules: Iterable[str]) -> List[str]:
        """Return the list of modules that could not be imported."""

        missing: List[str] = []
        for dotted_path in modules:
            if not dotted_path:
                continue
            try:
                importlib.import_module(dotted_path)
            except ImportError as exc:
                logger.debug(
                    "[Deps] import check failed for %s: %s", dotted_path, exc, exc_info=True
                )
                missing.append(dotted_path)
        return missing

    @staticmethod
    def is_installed(packages: Sequence[str]) -> bool:
        """Return ``True`` when all packages can be imported."""

        for package in packages:
            if not package:
                continue
            if importlib.util.find_spec(package) is None:  # type: ignore[attr-defined]
                return False
        return True


__all__ = ["DependencyManager"]
