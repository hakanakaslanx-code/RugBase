"""JSON lines based offline queue used when network connectivity is lost."""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Callable, List, Mapping, Sequence

from core import app_paths


class OutboxQueue:
    """Persist sync payloads to disk when Google Sheets is unreachable."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or app_paths.data_path("outbox.jsonl")
        self._lock = threading.Lock()
        if self._path.parent and not self._path.parent.exists():
            self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def append(self, entries: Sequence[Mapping[str, object]]) -> None:
        if not entries:
            return
        serialised = [json.dumps(entry, ensure_ascii=False) for entry in entries]
        with self._lock:
            with self._path.open("a", encoding="utf-8") as handle:
                for line in serialised:
                    handle.write(line)
                    handle.write("\n")

    def drain(self, handler: Callable[[Mapping[str, object]], None]) -> int:
        """Replay queued entries invoking ``handler`` for each payload."""

        with self._lock:
            if not self._path.exists():
                return 0
            with self._path.open("r", encoding="utf-8") as handle:
                lines = handle.readlines()

        entries: List[Mapping[str, object]] = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                entries.append(payload)

        if not entries:
            with self._lock:
                try:
                    self._path.unlink()
                except FileNotFoundError:
                    pass
            return 0

        remaining: List[Mapping[str, object]] = []
        sent = 0
        for payload in entries:
            try:
                handler(payload)
            except Exception:
                remaining.append(payload)
            else:
                sent += 1

        with self._lock:
            if not remaining:
                try:
                    self._path.unlink()
                except FileNotFoundError:
                    pass
            else:
                with self._path.open("w", encoding="utf-8") as handle:
                    for payload in remaining:
                        handle.write(json.dumps(payload, ensure_ascii=False))
                        handle.write("\n")

        return sent


__all__ = ["OutboxQueue"]
