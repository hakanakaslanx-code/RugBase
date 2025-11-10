import subprocess
import sys
import types
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest

import build_exe


def _install_pyinstaller_stub(monkeypatch):
    pyinstaller_module = types.ModuleType("PyInstaller")
    pyinstaller_main = types.ModuleType("PyInstaller.__main__")
    captured = {}

    def fake_run(args):
        captured["args"] = args

    pyinstaller_main.run = fake_run
    monkeypatch.setitem(sys.modules, "PyInstaller", pyinstaller_module)
    monkeypatch.setitem(sys.modules, "PyInstaller.__main__", pyinstaller_main)
    setattr(pyinstaller_module, "__main__", pyinstaller_main)
    return captured


def _mock_subprocess_run(monkeypatch, returncodes):
    calls = []

    def fake_run(cmd, check=False, stdout=None, stderr=None):
        calls.append(tuple(cmd))
        code = returncodes.pop(0)
        result = types.SimpleNamespace(returncode=code)
        if check and code != 0:
            raise subprocess.CalledProcessError(code, cmd)
        return result

    monkeypatch.setattr(build_exe.subprocess, "run", fake_run)
    return calls


def test_run_without_install(monkeypatch):
    captured = _install_pyinstaller_stub(monkeypatch)
    calls = _mock_subprocess_run(monkeypatch, [0])
    pip_calls = []

    def fake_check_call(cmd):
        pip_calls.append(tuple(cmd))
        return 0

    monkeypatch.setattr(build_exe.subprocess, "check_call", fake_check_call)

    build_exe.run()

    assert pip_calls == []
    assert calls == [
        (
            sys.executable,
            "-c",
            "import googleapiclient.discovery; import google.oauth2.service_account",
        )
    ]
    assert "args" in captured
    assert "--name=RugBase" in captured["args"][0]


def test_run_installs_missing_google_deps(monkeypatch):
    captured = _install_pyinstaller_stub(monkeypatch)
    calls = _mock_subprocess_run(monkeypatch, [1, 0])
    pip_calls = []

    def fake_check_call(cmd):
        pip_calls.append(tuple(cmd))
        return 0

    monkeypatch.setattr(build_exe.subprocess, "check_call", fake_check_call)

    build_exe.run()

    requirements = build_exe.pathlib.Path(build_exe.__file__).resolve().parent / "requirements.txt"
    assert pip_calls == [
        (
            sys.executable,
            "-m",
            "pip",
            "install",
            "-r",
            str(requirements),
        )
    ]
    assert len(calls) == 2
    assert "args" in captured


def test_run_raises_when_google_deps_still_missing(monkeypatch):
    _install_pyinstaller_stub(monkeypatch)
    _mock_subprocess_run(monkeypatch, [1, 1])

    def fake_check_call(cmd):
        return 0

    monkeypatch.setattr(build_exe.subprocess, "check_call", fake_check_call)

    with pytest.raises(SystemExit) as excinfo:
        build_exe.run()

    assert "Google dependencies could not be imported" in str(excinfo.value)


def test_run_raises_when_install_fails(monkeypatch):
    _install_pyinstaller_stub(monkeypatch)
    _mock_subprocess_run(monkeypatch, [1])

    def fake_check_call(cmd):
        raise build_exe.subprocess.CalledProcessError(1, cmd)

    monkeypatch.setattr(build_exe.subprocess, "check_call", fake_check_call)

    with pytest.raises(SystemExit) as excinfo:
        build_exe.run()

    assert "could not be installed automatically" in str(excinfo.value)
