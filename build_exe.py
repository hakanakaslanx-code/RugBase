"""Utilities for building a standalone RugBase executable with PyInstaller."""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys

from dependency_loader import HIDDEN_IMPORTS
from core import deps_bootstrap


def _hidden_import_args() -> list[str]:
    args: list[str] = []
    for module in HIDDEN_IMPORTS:
        args.append(f"--hidden-import={module}")
    return args


def _missing_runtime_dependencies() -> list[str]:
    missing: list[str] = []
    missing.extend(deps_bootstrap.check_google_deps())
    missing.extend(deps_bootstrap.check_pillow_deps())
    return missing


def _google_imports_available() -> bool:
    command = [
        sys.executable,
        "-c",
        "import googleapiclient.discovery; import google.oauth2.service_account",
    ]
    result = subprocess.run(command)
    if result.returncode != 0:
        return False
    return True


def _install_requirements(project_dir: pathlib.Path) -> None:
    requirements = project_dir / "requirements.txt"
    if not requirements.exists():  # pragma: no cover - defensive guard
        raise SystemExit(
            "requirements.txt could not be found. Install the Google dependencies manually."
        )

    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "-r", str(requirements)]
    )


def _prepare_runtime_dependencies(project_dir: pathlib.Path) -> None:
    def _runtime_ready() -> bool:
        if not _google_imports_available():
            return False
        return not _missing_runtime_dependencies()

    if _runtime_ready():
        return

    try:
        _install_requirements(project_dir)
    except subprocess.CalledProcessError as exc:
        raise SystemExit(
            "Required runtime dependencies could not be installed automatically. Run 'pip install -r requirements.txt' manually."
        ) from exc

    if not _runtime_ready():
        raise SystemExit(
            "Google dependencies could not be imported. Install the missing libraries before packaging again."
        )


def run() -> None:
    project_dir = pathlib.Path(__file__).resolve().parent
    entry_point = project_dir / "app.py"

    data_sep = os.pathsep

    _prepare_runtime_dependencies(project_dir)

    try:
        import PyInstaller.__main__  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - runtime guard
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
        except subprocess.CalledProcessError as install_exc:
            raise SystemExit(
                "PyInstaller is required to build the executable and could not be installed automatically."
            ) from install_exc
        import PyInstaller.__main__  # type: ignore

    args = [
        "--name=RugBase",
        "--onefile",
        "--windowed",
        *(_hidden_import_args()),
        "--collect-submodules=googleapiclient",
        "--collect-submodules=google",
        "--collect-submodules=google.oauth2",
        "--collect-all=PIL",
        f"--add-data={project_dir / 'core'}{data_sep}core",
        f"--add-data={project_dir / 'ui_item_card.py'}{data_sep}.",
        f"--add-data={project_dir / 'ui_main.py'}{data_sep}.",
        str(entry_point),
    ]

    PyInstaller.__main__.run(args)


if __name__ == "__main__":
    run()
