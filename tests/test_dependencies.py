import sys

from core.dependencies import DependencyManager


def test_add_to_sys_path(monkeypatch, tmp_path):
    site_packages = tmp_path / "site-packages"
    log_dir = tmp_path / "logs"

    monkeypatch.setattr(DependencyManager, "install_target", site_packages)
    monkeypatch.setattr(DependencyManager, "log_directory", log_dir)
    monkeypatch.setattr(sys, "path", [])

    result = DependencyManager.add_to_sys_path()

    assert result is True
    assert sys.path[0] == str(site_packages)


def test_verify_imports_reports_missing(monkeypatch):
    missing = DependencyManager.verify_imports(["math", "not_a_real_module_123"])

    assert missing == ["not_a_real_module_123"]
