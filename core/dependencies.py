"""Utility helpers for installing optional runtime dependencies."""
from __future__ import annotations

import importlib
import importlib.util
import logging
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

from core import app_paths

logger = logging.getLogger(__name__)


class DependencyManager:
    """Manage third-party dependencies shipped outside the executable."""

    install_target: Path = app_paths.DEPENDENCY_DIR
    log_directory: Path = app_paths.LOG_DIR
    pip_timeout: int = 300

    @classmethod
    def ensure_paths(cls) -> None:
        """Create the dependency and log directories."""

        app_paths.ensure_directory(cls.install_target)
        app_paths.ensure_directory(cls.log_directory)
        logger.info("Dependency target directory: %s", cls.install_target)

    @classmethod
    def add_to_sys_path(cls) -> bool:
        """Prepend the dependency directory to ``sys.path`` if it exists."""

        cls.ensure_paths()
        target = str(cls.install_target)
        if not cls.install_target.exists():
            logger.debug("Dependency target not yet available: %s", target)
            return False
        if target not in sys.path:
            sys.path.insert(0, target)
            logger.info("Dependency target added to sys.path: %s", target)
        else:
            logger.debug("Dependency target already present on sys.path: %s", target)
        logger.info("sys.path snapshot after dependency setup:\n%s", "\n".join(sys.path))
        return True

    @staticmethod
    def verify_imports(modules: Iterable[str]) -> List[str]:
        """Return a list of modules that could not be imported."""

        missing: List[str] = []
        for dotted_path in modules:
            if not dotted_path:
                continue
            try:
                importlib.import_module(dotted_path)
            except ImportError as exc:
                logger.debug("Import check failed for %s: %s", dotted_path, exc, exc_info=True)
                missing.append(dotted_path)
        return missing

    @staticmethod
    def is_installed(packages: Iterable[str]) -> bool:
        """Return ``True`` if all dotted-path packages can be imported."""

        for package in packages:
            if not package:
                continue
            if importlib.util.find_spec(package) is None:
                return False
        return True

    @classmethod
    def pip_install(
        cls, packages: Sequence[str], *, timeout: int | None = None
    ) -> Tuple[bool, str]:
        """Install ``packages`` into the managed dependency directory."""

        cls.ensure_paths()
        filtered = [package for package in packages if package]
        if not filtered:
            logger.debug("No packages specified for pip installation")
            return True, ""

        timeout = timeout or cls.pip_timeout
        command = [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "--no-input",
            "--disable-pip-version-check",
            "--no-color",
            "--no-warn-script-location",
            "--target",
            str(cls.install_target),
            *filtered,
        ]
        logger.info("Installing dependencies with pip: %s", ", ".join(filtered))

        try:
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:  # pragma: no cover - defensive
            output = ((exc.stdout or "") + (exc.stderr or "")).strip()
            logger.error(
                "pip install timed out after %s seconds for packages: %s",
                timeout,
                ", ".join(filtered),
            )
            if output:
                logger.error("pip output before timeout:\n%s", output)
            return False, output
        except OSError as exc:  # pragma: no cover - environment dependent
            logger.error("Failed to invoke pip: %s", exc)
            return False, str(exc)

        output = (result.stdout or "").strip()
        if output:
            logger.info("pip install output:\n%s", output)

        if result.returncode != 0:
            logger.error(
                "pip install failed with exit code %s for packages: %s",
                result.returncode,
                ", ".join(filtered),
            )
            return False, output

        cls.add_to_sys_path()
        importlib.invalidate_caches()
        logger.info(
            "sys.path snapshot after successful pip install:\n%s",
            "\n".join(sys.path),
        )
        return True, output


__all__ = ["DependencyManager"]
