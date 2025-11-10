"""Utilities for tracking and exposing synchronisation conflicts."""
from __future__ import annotations

import json
import logging
import threading
from collections import deque
from datetime import datetime
from typing import Deque, Dict, Iterable, List, Mapping, Optional, Tuple

from core import app_paths

_LOGGER = logging.getLogger("rugbase.sync.conflicts")
_HANDLER_CONFIGURED = False
_CONFLICTS: Deque[Dict[str, object]] = deque(maxlen=50)
_LOCK = threading.Lock()


def _ensure_logger() -> logging.Logger:
    global _HANDLER_CONFIGURED
    if not _HANDLER_CONFIGURED:
        path = app_paths.logs_path("conflicts.log")
        try:
            handler = logging.FileHandler(path, encoding="utf-8")
        except OSError:  # pragma: no cover - depends on filesystem permissions
            handler = None
        else:
            handler.setFormatter(
                logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
            )
            _LOGGER.addHandler(handler)
        _LOGGER.setLevel(logging.INFO)
        _HANDLER_CONFIGURED = True
    return _LOGGER


def record(
    row_id: str,
    field_diffs: Mapping[str, Tuple[str, str]],
    *,
    source: str = "merge",
    context: Optional[Mapping[str, object]] = None,
) -> None:
    """Persist a conflict entry and append it to the in-memory cache."""

    if not field_diffs:
        return
    timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    payload: Dict[str, object] = {
        "row_id": row_id,
        "fields": {key: list(value) for key, value in field_diffs.items()},
        "timestamp": timestamp,
        "source": source,
    }
    if context:
        payload.update(dict(context))

    logger = _ensure_logger()
    try:
        logger.info("%s", json.dumps(payload, ensure_ascii=False, sort_keys=True))
    except TypeError:  # pragma: no cover - defensive guard for non-serialisable data
        logger.info(
            "row_id=%s source=%s fields=%s", row_id, source, dict(field_diffs)
        )

    with _LOCK:
        _CONFLICTS.appendleft(payload)


def recent(limit: int = 10) -> List[Dict[str, object]]:
    """Return the most recent conflict entries."""

    with _LOCK:
        return list(list(_CONFLICTS)[:limit])


def clear() -> None:
    """Remove all cached conflict entries."""

    with _LOCK:
        _CONFLICTS.clear()


__all__ = ["record", "recent", "clear"]
