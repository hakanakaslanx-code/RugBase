import dependency_loader
dependency_loader.bootstrap()

import tkinter as tk
from tkinter import messagebox
from typing import Optional

import db
from core import deps_bootstrap
from core.dependencies import DependencyManager
from core.logging_config import configure_logging
from core.version import __version__
from core.single_instance import SingleInstanceError, acquire_instance_lock
from ui_main import MainWindow
from core.sheets_client import SheetsClientError, SheetsCredentialsError


def _notify_already_running() -> None:
    try:
        root = tk.Tk()
        root.withdraw()
        messagebox.showwarning("RugBase Inventory", "RugBase is already running.")
        root.destroy()
    except Exception:
        print("RugBase is already running.")


configure_logging()
deps_bootstrap.ensure_google_deps()
deps_bootstrap.ensure_pillow_deps()


def main() -> None:
    DependencyManager.add_to_sys_path()
    try:
        instance_lock = acquire_instance_lock("RugBase")
    except SingleInstanceError:
        _notify_already_running()
        return

    with instance_lock:
        initial_online = True
        init_error: Optional[str] = None
        force_sync_settings = False
        try:
            column_changes = db.initialize_database()
        except SheetsCredentialsError as exc:
            column_changes = []
            initial_online = False
            init_error = str(exc)
            force_sync_settings = True
        except SheetsClientError as exc:
            column_changes = []
            initial_online = False
            init_error = str(exc)

        root = tk.Tk()
        root.title(f"RugBase Inventory v{__version__}")
        root.geometry("1000x600")
        MainWindow(
            root,
            column_changes=column_changes,
            initial_online=initial_online,
            initial_error=init_error,
            force_sync_settings=force_sync_settings,
        )
        root.mainloop()


if __name__ == "__main__":
    main()
