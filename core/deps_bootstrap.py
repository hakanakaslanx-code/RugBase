"""Bootstrap Google API client dependencies at runtime.

This module ensures that the Google Drive/Sheets requirements are installed in
an isolated directory that ships alongside the RugBase executable.  It installs
online when possible and falls back to pre-packaged wheel files when the
machine is offline.  Detailed logging is emitted for diagnostics.
"""
from __future__ import annotations

import ensurepip
import importlib
import json
import logging
import os
import site
import sys
from pathlib import Path
from typing import List, Sequence

from core import app_paths

logger = logging.getLogger(__name__)

REQUIRED_PACKAGES: Sequence[str] = (
    "google-api-python-client",
    "google-auth-oauthlib",
    "google-auth",
    "httplib2",
    "google-auth-httplib2",
)

IMPORT_CHECKS: Sequence[str] = (
    "googleapiclient.discovery",
    "googleapiclient.http",
    "googleapiclient._auth",
    "google.auth",
    "google.auth.transport.requests",
    "google.oauth2.service_account",
    "google_auth_oauthlib.flow",
    "httplib2",
)

_SENTINEL_NAME = ".install_sentinel"
_ENV_LOGGED = False


def _dependency_dir() -> Path:
    return app_paths.dependencies_path()


def _resource_wheels_dir() -> Path:
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent.parent))
    candidate = base / "resources" / "wheels"
    return candidate


def _log_environment() -> None:
    global _ENV_LOGGED
    if _ENV_LOGGED:
        return
    _ENV_LOGGED = True

    logger.info("Python executable: %s", sys.executable)
    logger.info("Python version: %s", sys.version.replace("\n", " "))
    logger.info("sys.path (first 5): %s", sys.path[:5])
    try:
        user_site = site.getusersitepackages()
    except Exception:  # pragma: no cover - platform specific
        user_site = "<unavailable>"
    logger.info("User site-packages: %s", user_site)
    logger.info("Environment PYTHONHOME=%s", os.environ.get("PYTHONHOME"))
    logger.info("Environment PYTHONPATH=%s", os.environ.get("PYTHONPATH"))


def _add_dependency_path(target: Path) -> None:
    try:
        site.addsitedir(str(target))
    except Exception:  # pragma: no cover - defensive
        logger.debug("Failed to register dependency directory with addsitedir", exc_info=True)
    if str(target) not in sys.path:
        sys.path.insert(0, str(target))
    logger.debug("Dependency directory ensured on sys.path: %s", target)


def _write_sentinel(directory: Path, status: str, attempts: int) -> None:
    data = {
        "status": status,
        "attempts": attempts,
        "python": sys.version,
        "packages": list(REQUIRED_PACKAGES),
    }
    try:
        sentinel = directory / _SENTINEL_NAME
        directory.mkdir(parents=True, exist_ok=True)
        sentinel.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except OSError:
        logger.debug("Unable to write installation sentinel", exc_info=True)


def _ensure_pip() -> None:
    try:
        ensurepip.bootstrap()
    except Exception:  # pragma: no cover - environment dependent
        logger.debug("ensurepip.bootstrap() failed", exc_info=True)
    try:
        import pip  # noqa: F401  - ensure pip is importable
    except Exception:  # pragma: no cover - environment dependent
        logger.error("pip module could not be imported after ensurepip bootstrap", exc_info=True)
        raise


def _run_pip_install(args: Sequence[str]) -> bool:
    _ensure_pip()
    from pip._internal import main as pip_main  # type: ignore

    logger.info("Invoking pip with arguments: %s", " ".join(args))
    result = pip_main(list(args))
    if result != 0:
        logger.error("pip install failed with exit code %s", result)
        return False
    return True


def _install_online(target: Path) -> bool:
    args = [
        "install",
        "--no-cache-dir",
        "--disable-pip-version-check",
        "--target",
        str(target),
        *REQUIRED_PACKAGES,
    ]
    return _run_pip_install(args)


def _available_wheels() -> List[Path]:
    wheel_dir = _resource_wheels_dir()
    if not wheel_dir.exists():
        logger.warning("Wheel directory %s does not exist for offline install", wheel_dir)
        return []
    wheels = sorted(path for path in wheel_dir.glob("*.whl") if path.is_file())
    logger.info("Discovered %d pre-bundled wheels", len(wheels))
    return wheels


def _install_offline(target: Path) -> bool:
    wheels = _available_wheels()
    if not wheels:
        return False
    args = [
        "install",
        "--no-cache-dir",
        "--disable-pip-version-check",
        "--no-index",
        "--find-links",
        str(wheels[0].parent),
        "--target",
        str(target),
    ]
    args.extend(str(wheel) for wheel in wheels)
    return _run_pip_install(args)


def _verify_imports() -> bool:
    failures: List[str] = []
    for module_name in IMPORT_CHECKS:
        try:
            importlib.import_module(module_name)
        except Exception as exc:  # pragma: no cover - runtime dependent
            failures.append(f"{module_name}: {exc}")
            logger.error("Import failed for %s", module_name, exc_info=True)
    if failures:
        logger.warning("Dependency verification failures: %s", "; ".join(failures))
        return False
    logger.info("Google API dependencies verified successfully")
    return True


def _attempt_installation(target: Path) -> bool:
    attempts = 0
    for installer in (_install_online, _install_offline):
        attempts += 1
        try:
            success = installer(target)
        except Exception:  # pragma: no cover - pip level errors
            logger.error("Installer %s raised an exception", installer.__name__, exc_info=True)
            success = False
        if not success:
            logger.warning("%s attempt %d failed", installer.__name__, attempts)
            continue
        _add_dependency_path(target)
        if _verify_imports():
            _write_sentinel(target, "success", attempts)
            return True
    _write_sentinel(target, "failed", attempts)
    return False


def ensure_google_deps(force_reinstall: bool = False) -> bool:
    """Ensure Google API dependencies are importable.

    Parameters
    ----------
    force_reinstall:
        If ``True`` the dependencies will be reinstalled even if they already
        import successfully.
    """

    _log_environment()
    target = _dependency_dir()
    target.mkdir(parents=True, exist_ok=True)
    _add_dependency_path(target)

    if not force_reinstall and _verify_imports():
        return True

    logger.info("Installing Google API dependencies into %s", target)
    if _attempt_installation(target):
        return True

    # Retry once more after logging the traceback to help debugging.
    logger.error("First installation attempt failed; retrying once more")
    if _attempt_installation(target):
        return True

    logger.error(
        "Google API dependencies could not be installed. Check logs for detailed errors."
    )
    return False


__all__ = ["ensure_google_deps"]
