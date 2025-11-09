"""Utilities for building a standalone RugBase executable with PyInstaller."""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys

from dependency_loader import HIDDEN_IMPORTS


def _hidden_import_args() -> list[str]:
    args: list[str] = []
    for module in HIDDEN_IMPORTS:
        args.append(f"--hidden-import={module}")
    return args


def run() -> None:
    try:
        import PyInstaller.__main__  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
        raise SystemExit(
            "PyInstaller is required to build the executable. Install it with 'pip install pyinstaller'."
        ) from exc

    project_dir = pathlib.Path(__file__).resolve().parent
    entry_point = project_dir / "app.py"

    data_sep = os.pathsep

    check_cmd = [
        sys.executable,
        "-c",
        "import googleapiclient.discovery; import google.oauth2.service_account",
    ]
    try:
        subprocess.run(check_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as exc:
        raise SystemExit(
            "Google bağımlılıkları import edilemedi. 'pip install -r requirements.txt' çalıştırın."
        ) from exc

    args = [
        "--name=RugBase",
        "--onefile",
        "--windowed",
        *(_hidden_import_args()),
        "--collect-submodules=googleapiclient",
        "--collect-submodules=google",
        "--collect-submodules=google.oauth2",
        f"--add-data={project_dir / 'core'}{data_sep}core",
        f"--add-data={project_dir / 'ui_item_card.py'}{data_sep}.",
        f"--add-data={project_dir / 'ui_main.py'}{data_sep}.",
        str(entry_point),
    ]

    PyInstaller.__main__.run(args)


if __name__ == "__main__":
    run()
