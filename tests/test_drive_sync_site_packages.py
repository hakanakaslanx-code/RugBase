"""Tests for the site-packages refresh helper used by Google Drive sync."""

import importlib
import os
import sys

import pytest

from core import drive_sync


@pytest.fixture
def restore_sys_path():
    original = list(sys.path)
    try:
        yield
    finally:
        sys.path[:] = original


def test_refresh_site_packages_uses_addsitedir(monkeypatch, tmp_path, restore_sys_path):
    calls: list[str] = []

    def fake_iter_site_directories():
        yield str(tmp_path)

    def fake_isdir(path: str) -> bool:
        return True

    def fake_addsitedir(path: str) -> None:
        calls.append(path)
        if path not in sys.path:
            sys.path.append(path)

    monkeypatch.setattr(drive_sync, "_iter_site_directories", fake_iter_site_directories)
    monkeypatch.setattr(os.path, "isdir", fake_isdir)
    monkeypatch.setattr(importlib, "invalidate_caches", lambda: None)
    monkeypatch.setattr(drive_sync.site, "addsitedir", fake_addsitedir)

    drive_sync._refresh_site_packages()

    assert calls == [str(tmp_path)]
    assert str(tmp_path) in sys.path


def test_refresh_site_packages_logs_failures(monkeypatch, caplog, restore_sys_path, tmp_path):
    def fake_iter_site_directories():
        yield str(tmp_path)

    def fake_isdir(path: str) -> bool:
        return True

    def fake_addsitedir(path: str) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(drive_sync, "_iter_site_directories", fake_iter_site_directories)
    monkeypatch.setattr(os.path, "isdir", fake_isdir)
    monkeypatch.setattr(importlib, "invalidate_caches", lambda: None)
    monkeypatch.setattr(drive_sync.site, "addsitedir", fake_addsitedir)

    with caplog.at_level("DEBUG"):
        drive_sync._refresh_site_packages()

    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert "Failed to register site-packages directory" in messages
