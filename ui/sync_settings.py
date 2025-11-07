"""Tkinter UI for managing Google Drive synchronisation settings."""
from __future__ import annotations

import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable, Dict, Optional

import db
from core import sync


class SyncSettingsWindow:
    def __init__(self, parent: tk.Misc, on_saved: Optional[Callable[[], None]] = None) -> None:
        self.parent = parent
        self.on_saved = on_saved
        self.settings = sync.load_settings()

        self.window = tk.Toplevel(parent)
        self.window.title("Sync Settings")
        self.window.transient(parent)
        self.window.grab_set()
        self.window.resizable(False, False)

        self._build_ui()
        self._populate_fields()
        self._refresh_conflicts()

    def _build_ui(self) -> None:
        padding = {"padx": 10, "pady": 6}
        self.main_frame = ttk.Frame(self.window, padding=10)
        self.main_frame.grid(row=0, column=0, sticky="nsew")

        # Credentials frame
        creds_frame = ttk.LabelFrame(self.main_frame, text="Google Drive Credentials", padding=10)
        creds_frame.grid(row=0, column=0, sticky="ew", **padding)

        ttk.Label(creds_frame, text="Client Secret (client_secret.json):").grid(row=0, column=0, sticky="w")
        self.client_secret_var = tk.StringVar()
        client_entry = ttk.Entry(creds_frame, textvariable=self.client_secret_var, width=50)
        client_entry.grid(row=1, column=0, sticky="ew", pady=(2, 6))
        browse_button = ttk.Button(creds_frame, text="Browse…", command=self._on_browse_client_secret)
        browse_button.grid(row=1, column=1, padx=(8, 0))

        ttk.Label(creds_frame, text="Token Path:").grid(row=2, column=0, sticky="w")
        self.token_path_var = tk.StringVar()
        token_entry = ttk.Entry(creds_frame, textvariable=self.token_path_var, width=50, state="readonly")
        token_entry.grid(row=3, column=0, sticky="ew", pady=(2, 6))
        ttk.Label(creds_frame, text="Root Folder ID (optional):").grid(row=4, column=0, sticky="w")
        self.root_folder_id_var = tk.StringVar()
        ttk.Entry(creds_frame, textvariable=self.root_folder_id_var, width=50).grid(row=5, column=0, sticky="ew", pady=(2, 6))

        ttk.Label(creds_frame, text="Root Folder Name:").grid(row=6, column=0, sticky="w")
        self.root_folder_name_var = tk.StringVar()
        ttk.Entry(creds_frame, textvariable=self.root_folder_name_var, width=50).grid(row=7, column=0, sticky="ew", pady=(2, 0))

        creds_frame.columnconfigure(0, weight=1)

        # Behaviour frame
        behaviour_frame = ttk.LabelFrame(self.main_frame, text="Behaviour", padding=10)
        behaviour_frame.grid(row=1, column=0, sticky="ew", **padding)

        ttk.Label(behaviour_frame, text="Polling Interval (seconds):").grid(row=0, column=0, sticky="w")
        self.poll_interval_var = tk.StringVar()
        ttk.Entry(behaviour_frame, textvariable=self.poll_interval_var, width=12).grid(row=0, column=1, padx=(8, 0))

        ttk.Label(behaviour_frame, text="Node Name:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.node_name_var = tk.StringVar()
        ttk.Entry(behaviour_frame, textvariable=self.node_name_var, width=30).grid(row=1, column=1, padx=(8, 0), pady=(6, 0))

        ttk.Label(behaviour_frame, text="Last Sync Time:").grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.last_sync_var = tk.StringVar()
        ttk.Label(behaviour_frame, textvariable=self.last_sync_var).grid(row=2, column=1, sticky="w", pady=(8, 0))

        ttk.Label(behaviour_frame, text="Changelog Folder ID:").grid(row=3, column=0, sticky="w", pady=(6, 0))
        self.changelog_var = tk.StringVar()
        ttk.Entry(behaviour_frame, textvariable=self.changelog_var, width=40, state="readonly").grid(
            row=3, column=1, sticky="ew", pady=(6, 0)
        )

        ttk.Label(behaviour_frame, text="Backups Folder ID:").grid(row=4, column=0, sticky="w", pady=(6, 0))
        self.backups_var = tk.StringVar()
        ttk.Entry(behaviour_frame, textvariable=self.backups_var, width=40, state="readonly").grid(
            row=4, column=1, sticky="ew", pady=(6, 0)
        )

        behaviour_frame.columnconfigure(1, weight=1)

        # Action buttons
        button_frame = ttk.Frame(self.main_frame)
        button_frame.grid(row=2, column=0, sticky="ew", **padding)
        button_frame.columnconfigure(0, weight=1)

        self.status_var = tk.StringVar()
        ttk.Label(button_frame, textvariable=self.status_var, foreground="#0b5394").grid(
            row=0, column=0, sticky="w"
        )

        ttk.Button(button_frame, text="Test Connection", command=self._on_test_connection).grid(
            row=0, column=1, padx=(6, 0)
        )
        ttk.Button(button_frame, text="Save", command=self._on_save).grid(row=0, column=2, padx=(6, 0))
        ttk.Button(button_frame, text="Close", command=self.window.destroy).grid(row=0, column=3, padx=(6, 0))

        # Conflicts
        conflicts_frame = ttk.LabelFrame(self.main_frame, text="Conflicts", padding=10)
        conflicts_frame.grid(row=3, column=0, sticky="nsew", **padding)
        conflicts_frame.columnconfigure(0, weight=1)
        conflicts_frame.rowconfigure(0, weight=1)

        columns = ("id", "item", "reason", "created")
        self.conflicts_tree = ttk.Treeview(conflicts_frame, columns=columns, show="headings", height=6)
        self.conflicts_tree.heading("id", text="ID")
        self.conflicts_tree.heading("item", text="Item")
        self.conflicts_tree.heading("reason", text="Reason")
        self.conflicts_tree.heading("created", text="Created")
        self.conflicts_tree.column("id", width=60, anchor="center")
        self.conflicts_tree.column("item", width=120)
        self.conflicts_tree.column("reason", width=260)
        self.conflicts_tree.column("created", width=160)
        self.conflicts_tree.grid(row=0, column=0, sticky="nsew")

        scrollbar = ttk.Scrollbar(conflicts_frame, orient="vertical", command=self.conflicts_tree.yview)
        self.conflicts_tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.grid(row=0, column=1, sticky="ns")

        conflict_button_frame = ttk.Frame(conflicts_frame)
        conflict_button_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        conflict_button_frame.columnconfigure(0, weight=1)
        ttk.Button(conflict_button_frame, text="Resolve Selected", command=self._on_resolve_selected).grid(
            row=0, column=1, padx=(6, 0)
        )
        ttk.Button(conflict_button_frame, text="Refresh", command=self._refresh_conflicts).grid(
            row=0, column=2, padx=(6, 0)
        )

    def _populate_fields(self) -> None:
        self.client_secret_var.set(self.settings.get("client_secret_path", ""))
        self.token_path_var.set(self.settings.get("token_path", ""))
        self.root_folder_id_var.set(self.settings.get("root_folder_id", ""))
        self.root_folder_name_var.set(self.settings.get("root_folder_name", sync.DEFAULT_ROOT_NAME))
        self.poll_interval_var.set(str(self.settings.get("poll_interval", sync.DEFAULT_POLL_INTERVAL)))
        self.node_name_var.set(self.settings.get("node_name", ""))
        last_sync = self.settings.get("last_sync_time")
        self.last_sync_var.set(last_sync or "Never")
        self.changelog_var.set(self.settings.get("changelog_folder_id", ""))
        self.backups_var.set(self.settings.get("backups_folder_id", ""))

    def _collect_form_data(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "client_secret_path": self.client_secret_var.get().strip(),
            "token_path": self.token_path_var.get().strip() or self.settings.get("token_path"),
            "root_folder_id": self.root_folder_id_var.get().strip(),
            "root_folder_name": self.root_folder_name_var.get().strip() or sync.DEFAULT_ROOT_NAME,
            "poll_interval": self.poll_interval_var.get().strip(),
            "node_name": self.node_name_var.get().strip() or self.settings.get("node_name", ""),
            "changelog_folder_id": self.changelog_var.get().strip(),
            "backups_folder_id": self.backups_var.get().strip(),
        }
        return data

    def _on_browse_client_secret(self) -> None:
        initial_dir = os.path.dirname(self.client_secret_var.get()) or os.getcwd()
        file_path = filedialog.askopenfilename(
            parent=self.window,
            initialdir=initial_dir,
            filetypes=[("JSON Files", "*.json"), ("All Files", "*.*")],
        )
        if file_path:
            self.client_secret_var.set(file_path)

    def _on_test_connection(self) -> None:
        data = self._collect_form_data()
        try:
            structure = sync.test_connection(data)
        except sync.SyncConfigurationError as exc:
            messagebox.showerror("Sync Settings", str(exc), parent=self.window)
            return
        except Exception as exc:
            messagebox.showerror("Sync Settings", f"Connection failed: {exc}", parent=self.window)
            return

        self.root_folder_id_var.set(structure.get("root", ""))
        self.changelog_var.set(structure.get("changelog", ""))
        self.backups_var.set(structure.get("backups", ""))
        self.status_var.set("Drive connection successful.")

    def _on_save(self) -> None:
        data = self._collect_form_data()
        try:
            interval = int(data.get("poll_interval") or sync.DEFAULT_POLL_INTERVAL)
        except ValueError:
            messagebox.showerror("Sync Settings", "Polling interval must be a number.", parent=self.window)
            return

        if data["client_secret_path"] and not os.path.exists(data["client_secret_path"]):
            messagebox.showerror("Sync Settings", "Client secret file does not exist.", parent=self.window)
            return

        data["poll_interval"] = interval

        updated = dict(self.settings)
        updated.update(data)
        sync.save_settings(updated)
        self.settings = updated
        self.status_var.set("Settings saved.")

        if self.on_saved:
            self.on_saved()

    def _refresh_conflicts(self) -> None:
        for item in self.conflicts_tree.get_children():
            self.conflicts_tree.delete(item)

        conflicts = db.fetch_conflicts(resolved=False)
        for conflict in conflicts:
            self.conflicts_tree.insert(
                "",
                tk.END,
                values=(
                    conflict.get("id"),
                    conflict.get("item_id") or "-",
                    conflict.get("reason"),
                    conflict.get("created_at"),
                ),
            )

    def _on_resolve_selected(self) -> None:
        selection = self.conflicts_tree.selection()
        if not selection:
            messagebox.showinfo("Conflicts", "Select at least one conflict to resolve.", parent=self.window)
            return

        for item_id in selection:
            values = self.conflicts_tree.item(item_id, "values")
            if not values:
                continue
            try:
                conflict_id = int(values[0])
            except (TypeError, ValueError):
                continue
            db.resolve_conflict(conflict_id)

        self._refresh_conflicts()
        self.status_var.set("Selected conflicts marked as resolved.")
