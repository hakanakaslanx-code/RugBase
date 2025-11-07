"""Application-wide logging configuration utilities."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import db

_LOG_PATH: Optional[Path] = None


def configure_logging(level: int = logging.INFO) -> Path:
    """Configure logging to write to the RugBase log file.

    Parameters
    ----------
    level:
        The minimum logging level for the root logger. ``logging.INFO`` is used
        by default which captures operational details without being overly
        verbose.

    Returns
    -------
    pathlib.Path
        The path to the log file.
    """

    global _LOG_PATH

    if _LOG_PATH is not None:
        return _LOG_PATH

    log_path = Path(db.resource_path("rugbase.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        log_path.touch(exist_ok=True)
    except OSError:
        pass

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.setLevel(level)
    else:
        root_logger.setLevel(min(root_logger.level, level))

    already_configured = any(
        isinstance(handler, logging.FileHandler)
        and getattr(handler, "baseFilename", None) == str(log_path)
        for handler in root_logger.handlers
    )
    if not already_configured:
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    _LOG_PATH = log_path
    root_logger.debug("Logging configured. Writing to %s", log_path)
    return log_path


def get_log_path() -> Path:
    """Return the path to the RugBase log file, configuring logging if needed."""

    if _LOG_PATH is None:
        return configure_logging()
    return _LOG_PATH
