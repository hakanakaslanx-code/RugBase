"""Utilities for building a standalone RugBase executable with PyInstaller."""

from __future__ import annotations

import os
import pathlib


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

    PyInstaller.__main__.run(
        [
            "--name=RugBase",
            "--onefile",
            "--windowed",
            "--hidden-import=googleapiclient.discovery",
            "--hidden-import=googleapiclient.errors",
            "--hidden-import=googleapiclient.http",
            "--hidden-import=google.oauth2.credentials",
            "--hidden-import=google.auth.transport.requests",
            "--hidden-import=google_auth_oauthlib.flow",
            f"--add-data={project_dir / 'core'}{data_sep}core",
            f"--add-data={project_dir / 'ui_item_card.py'}{data_sep}.",
            f"--add-data={project_dir / 'ui_main.py'}{data_sep}.",
            str(entry_point),
        ]
    )


if __name__ == "__main__":
    run()
