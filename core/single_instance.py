"""Utilities for enforcing a single running RugBase instance."""
from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

if os.name == "nt":  # pragma: no cover - Windows specific branch
    import msvcrt
else:  # pragma: no cover - POSIX branch
    import fcntl  # type: ignore[import-not-found]


class SingleInstanceError(RuntimeError):
    """Raised when another RugBase instance is already running."""


@dataclass
class _InstanceLock:
    """Simple lock wrapper around a filesystem-backed mutex."""

    name: str
    _file_handle: Optional[object] = None
    _lock_path: Optional[Path] = None

    def acquire(self) -> "_InstanceLock":
        lock_filename = f"{self._sanitize_name(self.name)}.lock"
        lock_path = Path(tempfile.gettempdir()) / lock_filename
        lock_path.parent.mkdir(parents=True, exist_ok=True)

        file_handle = open(lock_path, "a+b")
        file_handle.seek(0)
        try:
            self._lock_file(file_handle)
        except OSError as exc:
            file_handle.close()
            raise SingleInstanceError(f"{self.name} is already running.") from exc

        try:
            file_handle.seek(0)
            file_handle.truncate()
            file_handle.write(str(os.getpid()).encode("utf-8"))
            file_handle.flush()
        except OSError:
            # If writing the PID fails we still hold the lock; ignore silently.
            pass

        self._file_handle = file_handle
        self._lock_path = lock_path
        return self

    def release(self) -> None:
        file_handle = self._file_handle
        if not file_handle:
            return

        try:
            try:
                file_handle.seek(0)
            except OSError:
                # If resetting the pointer fails we still try to release the lock.
                pass

            self._unlock_file(file_handle)
        finally:
            try:
                file_handle.close()
            finally:
                self._file_handle = None

        lock_path = self._lock_path
        if lock_path and lock_path.exists():
            try:
                lock_path.unlink()
            except OSError:
                pass
        self._lock_path = None

    def __enter__(self) -> "_InstanceLock":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        self.release()

    @staticmethod
    def _sanitize_name(name: str) -> str:
        safe = [char if char.isalnum() else "_" for char in name]
        value = "".join(safe).strip("_")
        return value or "rugbase"

    @staticmethod
    def _lock_file(handle: object) -> None:
        if os.name == "nt":  # pragma: no cover - Windows specific branch
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:  # pragma: no cover - POSIX branch
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _unlock_file(handle: object) -> None:
        if os.name == "nt":  # pragma: no cover - Windows specific branch
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:  # pragma: no cover - POSIX branch
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def acquire_instance_lock(name: str = "RugBase") -> _InstanceLock:
    """Attempt to acquire the single instance lock.

    :raises SingleInstanceError: when another instance is already running.
    :returns: a context-manageable lock object.
    """

    lock = _InstanceLock(name)
    return lock.acquire()


@contextmanager
def single_instance(name: str = "RugBase") -> Iterator[_InstanceLock]:
    """Context manager that ensures only one instance of RugBase runs."""

    lock = acquire_instance_lock(name)
    try:
        yield lock
    finally:
        lock.release()


__all__ = [
    "SingleInstanceError",
    "acquire_instance_lock",
    "single_instance",
]
