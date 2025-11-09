import importlib
import subprocess
import sys

from core.dependencies import DependencyManager


def test_pip_install_uses_managed_target(monkeypatch, tmp_path):
    site_packages = tmp_path / "site-packages"
    log_dir = tmp_path / "logs"

    monkeypatch.setattr(DependencyManager, "install_target", site_packages)
    monkeypatch.setattr(DependencyManager, "log_directory", log_dir)
    monkeypatch.setattr(importlib, "invalidate_caches", lambda: None)
    monkeypatch.setattr(sys, "path", [])

    class DummyResult:
        returncode = 0
        stdout = "ok"

    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return DummyResult()

    monkeypatch.setattr(subprocess, "run", fake_run)

    success, output = DependencyManager.pip_install(["example"], timeout=5)

    assert success
    assert output == "ok"
    assert captured["command"][0:3] == [sys.executable, "-m", "pip"]
    assert "--target" in captured["command"]
    assert str(site_packages) in captured["command"]
    assert sys.path[0] == str(site_packages)
