"""Utilities for building a standalone RugBase executable with PyInstaller."""

from __future__ import annotations

import os
import pathlib

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

    args = [
        "--name=RugBase",
        "--onefile",
        "--windowed",
        *(_hidden_import_args()),
        f"--add-data={project_dir / 'core'}{data_sep}core",
        f"--add-data={project_dir / 'ui_item_card.py'}{data_sep}.",
        f"--add-data={project_dir / 'ui_main.py'}{data_sep}.",
        str(entry_point),
    ]

    PyInstaller.__main__.run(args)


if __name__ == "__main__":
    run()
