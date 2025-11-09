"""Tkinter UI for managing Google Drive synchronisation settings."""
from __future__ import annotations

import json
import os
import platform
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable, Dict, Optional, Sequence

import db
from core import app_paths, deps_bootstrap, drive_sync, sheets_sync


DEPENDENCY_FAILURE_MESSAGE = (
    "google-api-python-client veya google-auth-oauthlib eksik. Kurulum paketini yeniden "
    "yükleyin veya geliştirici build'inde 'pip install -r requirements.txt' çalıştırıp "
    "PyInstaller ile paketleyin."
)


class SyncSettingsWindow:
    def __init__(self, parent: tk.Misc, on_saved: Optional[Callable[[], None]] = None) -> None:
        self.parent = parent
        self.on_saved = on_saved
        self.settings = drive_sync.load_settings()

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
        creds_frame = ttk.LabelFrame(self.main_frame, text="Google Drive & Sheets", padding=10)
        creds_frame.grid(row=0, column=0, sticky="ew", **padding)

        ttk.Label(creds_frame, text="Sheet URL / ID:").grid(row=0, column=0, sticky="w")
        self.sheet_url_var = tk.StringVar()
        sheet_entry = ttk.Entry(creds_frame, textvariable=self.sheet_url_var, width=52)
        sheet_entry.grid(row=0, column=1, columnspan=2, sticky="ew", pady=(0, 6))
        sheet_entry.bind("<FocusOut>", self._on_sheet_focus_out)

        ttk.Label(creds_frame, text="Hizmet hesabı e-postası:").grid(row=1, column=0, sticky="w")
        self.service_email_var = tk.StringVar()
        ttk.Entry(creds_frame, textvariable=self.service_email_var, width=52).grid(
            row=1, column=1, columnspan=2, sticky="ew", pady=(0, 6)
        )

        ttk.Label(creds_frame, text="Private Key ID (opsiyonel):").grid(row=2, column=0, sticky="w")
        self.private_key_var = tk.StringVar()
        ttk.Entry(creds_frame, textvariable=self.private_key_var, width=52).grid(
            row=2, column=1, columnspan=2, sticky="ew", pady=(0, 6)
        )

        ttk.Label(creds_frame, text="Service Account JSON:").grid(row=3, column=0, sticky="w")
        self.client_secret_var = tk.StringVar()
        client_entry = ttk.Entry(creds_frame, textvariable=self.client_secret_var, width=48)
        client_entry.grid(row=3, column=1, sticky="ew", pady=(0, 6))
        ttk.Button(creds_frame, text="Gözat…", command=self._on_browse_client_secret).grid(
            row=3, column=2, padx=(8, 0)
        )

        ttk.Label(creds_frame, text="Token Path:").grid(row=4, column=0, sticky="w")
        self.token_path_var = tk.StringVar()
        token_entry = ttk.Entry(creds_frame, textvariable=self.token_path_var, width=48)
        token_entry.grid(row=4, column=1, sticky="ew", pady=(0, 6))
        ttk.Button(creds_frame, text="Varsayılanı Kullan", command=self._on_reset_token_path).grid(
            row=4, column=2, padx=(8, 0)
        )

        ttk.Label(creds_frame, text="Root Folder ID:").grid(row=5, column=0, sticky="w")
        self.root_folder_id_var = tk.StringVar()
        ttk.Entry(
            creds_frame,
            textvariable=self.root_folder_id_var,
            width=52,
            state="readonly",
        ).grid(row=5, column=1, columnspan=2, sticky="ew", pady=(0, 0))

        creds_frame.columnconfigure(1, weight=1)

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

        self.dependency_status_var = tk.StringVar()
        self.connection_status_var = tk.StringVar()

        ttk.Label(
            button_frame,
            textvariable=self.dependency_status_var,
            foreground="#0b5394",
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            button_frame,
            textvariable=self.connection_status_var,
            foreground="#0b5394",
        ).grid(row=1, column=0, sticky="w", pady=(2, 0))

        ttk.Button(button_frame, text="Bağımlılık Testi", command=self._on_dependency_test).grid(
            row=0, column=1, padx=(6, 0)
        )
        ttk.Button(button_frame, text="Erişimi Doğrula", command=self._on_test_connection).grid(
            row=0, column=2, padx=(6, 0)
        )
        ttk.Button(button_frame, text="Kaydet", command=self._on_save).grid(
            row=1, column=1, padx=(6, 0), pady=(4, 0)
        )
        ttk.Button(button_frame, text="Kapat", command=self.window.destroy).grid(
            row=1, column=2, padx=(6, 0), pady=(4, 0)
        )

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
        sheet_url = self.settings.get("spreadsheet_url", drive_sync.DEFAULT_SPREADSHEET_URL)
        self.sheet_url_var.set(sheet_url)
        service_email = self.settings.get(
            "service_account_email", drive_sync.DEFAULT_SERVICE_ACCOUNT_EMAIL
        )
        self.service_email_var.set(service_email)
        self.private_key_var.set(
            self.settings.get("private_key_id", drive_sync.DEFAULT_PRIVATE_KEY_ID)
        )

        secret_path = self.settings.get("client_secret_path") or drive_sync.service_account_storage_path()
        self.client_secret_var.set(secret_path)

        token_path = self.settings.get("token_path") or str(
            app_paths.tokens_path(drive_sync.TOKEN_FILENAME)
        )
        self.token_path_var.set(token_path)
        self.root_folder_id_var.set(self.settings.get("root_folder_id", drive_sync.ROOT_FOLDER_ID))
        poll_default = self.settings.get("poll_interval", drive_sync.DEFAULT_POLL_INTERVAL)
        self.poll_interval_var.set(str(poll_default))
        node_default = self.settings.get("node_name") or platform.node() or "RugBaseNode"
        self.node_name_var.set(node_default)
        last_sync = self.settings.get("last_sync_time")
        self.last_sync_var.set(last_sync or "Never")
        self.changelog_var.set(self.settings.get("changelog_folder_id", ""))
        self.backups_var.set(self.settings.get("backups_folder_id", ""))
        self.connection_status_var.set("")
        self._update_dependency_status()

    def _collect_form_data(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "client_secret_path": self.client_secret_var.get().strip(),
            "token_path": self.token_path_var.get().strip()
            or str(app_paths.tokens_path(drive_sync.TOKEN_FILENAME)),
            "poll_interval": self.poll_interval_var.get().strip(),
            "node_name": self.node_name_var.get().strip() or self.settings.get("node_name", ""),
            "root_folder_id": self.root_folder_id_var.get().strip() or drive_sync.ROOT_FOLDER_ID,
            "changelog_folder_id": self.changelog_var.get().strip() or self.settings.get("changelog_folder_id"),
            "backups_folder_id": self.backups_var.get().strip() or self.settings.get("backups_folder_id"),
            "service_account_email": self.service_email_var.get().strip()
            or drive_sync.DEFAULT_SERVICE_ACCOUNT_EMAIL,
            "spreadsheet_url": self.sheet_url_var.get().strip()
            or drive_sync.DEFAULT_SPREADSHEET_URL,
            "private_key_id": self.private_key_var.get().strip(),
        }
        return data

    def _update_dependency_status(self, missing: Optional[Sequence[str]] = None) -> None:
        if missing is None:
            missing = deps_bootstrap.check_google_deps()
        if missing:
            summary = ", ".join(missing)
            self.dependency_status_var.set(f"Bağımlılık testi: FAIL ({summary})")
        else:
            self.dependency_status_var.set("Bağımlılık testi: PASS")

    def _on_dependency_test(self) -> None:
        missing = deps_bootstrap.check_google_deps()
        self._update_dependency_status(missing)
        if missing:
            messagebox.showerror(
                "Sync Settings",
                f"{DEPENDENCY_FAILURE_MESSAGE}\n\nEksik modüller: {', '.join(missing)}",
                parent=self.window,
            )
        else:
            messagebox.showinfo(
                "Sync Settings",
                "Tüm Google bağımlılıkları başarıyla import edildi.",
                parent=self.window,
            )

    def _on_reset_token_path(self) -> None:
        default_path = str(app_paths.tokens_path(drive_sync.TOKEN_FILENAME))
        self.token_path_var.set(default_path)

    def _on_sheet_focus_out(self, _event: tk.Event) -> None:
        value = self.sheet_url_var.get().strip()
        parsed = sheets_sync.parse_spreadsheet_id(value)
        if parsed and parsed != value:
            self.sheet_url_var.set(parsed)

    def _load_service_account_metadata(self, path: str) -> None:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, json.JSONDecodeError):
            return

        email = data.get("client_email")
        if isinstance(email, str) and email.strip():
            self.service_email_var.set(email.strip())

        private_key_id = data.get("private_key_id")
        if isinstance(private_key_id, str) and private_key_id.strip():
            self.private_key_var.set(private_key_id.strip())

    def _on_browse_client_secret(self) -> None:
        initial_dir = os.path.dirname(self.client_secret_var.get()) or os.getcwd()
        file_path = filedialog.askopenfilename(
            parent=self.window,
            initialdir=initial_dir,
            filetypes=[("JSON Files", "*.json"), ("All Files", "*.*")],
        )
        if file_path:
            self.client_secret_var.set(file_path)
            self._load_service_account_metadata(file_path)

    def _on_test_connection(self) -> None:
        self.connection_status_var.set("Bağlantı doğrulanıyor…")
        missing = deps_bootstrap.check_google_deps()
        if missing:
            self._update_dependency_status(missing)
            messagebox.showerror(
                "Sync Settings",
                f"{DEPENDENCY_FAILURE_MESSAGE}\n\nEksik modüller: {', '.join(missing)}",
                parent=self.window,
            )
            self.connection_status_var.set("Bağımlılık eksik")
            return

        data = self._collect_form_data()
        spreadsheet_id = sheets_sync.parse_spreadsheet_id(data["spreadsheet_url"])
        if not spreadsheet_id:
            messagebox.showwarning(
                "Sync Settings", "Geçerli bir Sheet URL/ID girin.", parent=self.window
            )
            self.connection_status_var.set("Geçersiz Sheet ID")
            return

        self.sheet_url_var.set(spreadsheet_id)
        data["spreadsheet_url"] = spreadsheet_id

        try:
            structure = drive_sync.test_connection(data)
        except drive_sync.SyncConfigurationError as exc:
            messagebox.showerror("Sync Settings", str(exc), parent=self.window)
            self.connection_status_var.set(str(exc))
            return
        except Exception as exc:
            messagebox.showerror(
                "Sync Settings", f"Erişim doğrulanamadı: {exc}", parent=self.window
            )
            self.connection_status_var.set("Erişim doğrulanamadı")
            return

        self.settings.update(data)
        self.root_folder_id_var.set(structure.get("root", ""))
        self.changelog_var.set(structure.get("changelog", ""))
        self.backups_var.set(structure.get("backups", ""))
        self.settings["root_folder_id"] = structure.get("root")
        self.settings["changelog_folder_id"] = structure.get("changelog")
        self.settings["backups_folder_id"] = structure.get("backups")

        credential_path = drive_sync.service_account_storage_path()
        if not credential_path or not os.path.exists(credential_path):
            credential_path = data["client_secret_path"]

        try:
            client = sheets_sync.get_client(credential_path)
            sheets_sync.ensure_sheet(client, spreadsheet_id)
        except sheets_sync.SpreadsheetAccessError as exc:
            email = data.get("service_account_email", drive_sync.DEFAULT_SERVICE_ACCOUNT_EMAIL)
            message = (
                "Sheet erişimi doğrulanamadı. Lütfen Google Sheet'i "
                f"'{email}' hizmet hesabı ile 'Editor' olarak paylaşın.\n\nDetay: {exc}"
            )
            messagebox.showwarning("Sync Settings", message, parent=self.window)
            self.connection_status_var.set("Sheet paylaşımı gerekli")
            return
        except sheets_sync.SheetsSyncError as exc:
            messagebox.showerror(
                "Sync Settings", f"Sheets doğrulaması başarısız: {exc}", parent=self.window
            )
            self.connection_status_var.set("Sheets doğrulaması başarısız")
            return

        self.connection_status_var.set("Drive ve Sheets erişimi doğrulandı")

    def _on_save(self) -> None:
        data = self._collect_form_data()
        try:
            interval = int(data.get("poll_interval") or drive_sync.DEFAULT_POLL_INTERVAL)
        except ValueError:
            messagebox.showerror("Sync Settings", "Polling interval must be a number.", parent=self.window)
            return

        storage_path = drive_sync.service_account_storage_path()
        client_candidate = data.get("client_secret_path")
        if client_candidate and client_candidate != storage_path and not os.path.exists(client_candidate):
            messagebox.showerror("Sync Settings", "Service account file does not exist.", parent=self.window)
            return

        data["poll_interval"] = interval

        updated = dict(self.settings)
        updated.update(data)
        try:
            drive_sync.save_settings(updated)
        except RuntimeError as exc:
            messagebox.showerror("Sync Settings", str(exc), parent=self.window)
            return
        self.settings = updated
        self.client_secret_var.set(
            self.settings.get("client_secret_path") or drive_sync.service_account_storage_path()
        )
        self.token_path_var.set(
            self.settings.get("token_path") or str(app_paths.tokens_path(drive_sync.TOKEN_FILENAME))
        )
        self.connection_status_var.set("Ayarlar kaydedildi.")

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
        self.connection_status_var.set("Seçilen çatışmalar çözüldü.")
