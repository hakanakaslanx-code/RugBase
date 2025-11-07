"""Hashing utilities for RugBase."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional


def file_sha256(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    """Return the SHA-256 hash of a file."""

    hasher = hashlib.sha256()
    with open(Path(path), "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def try_file_sha256(path: str | Path) -> Optional[str]:
    """Compute the SHA-256 hash of a file if it exists, otherwise return ``None``."""

    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        return None
    return file_sha256(file_path)
