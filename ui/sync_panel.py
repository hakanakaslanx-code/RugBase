"""Tkinter control panel for Google Sheets synchronisation."""
from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
from pathlib import Path
from typing import Callable, Optional

import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk

from core import sheets_sync
from core.google_credentials import CredentialsFileInvalidError, load_service_account_data
from core.auto_sync import AutoSyncController
from dependency_loader import dependency_warning
from settings import (
    DEFAULT_CREDENTIALS_PATH,
    DEFAULT_SERVICE_ACCOUNT_EMAIL,
    DEFAULT_SPREADSHEET_ID,
    DEFAULT_WORKSHEET_TITLE,
    GoogleSyncSettings,
    load_google_sync_settings,
    save_google_sync_settings,
)


class _BackgroundWorker:
    """Process synchronisation operations on a background thread."""

    def __init__(self, callback: Callable[[str, str, Optional[Exception], Optional[object]], None]) -> None:
        self._callback = callback
        self._queue: "queue.Queue[tuple[str, Callable[[], object]]]" = queue.Queue()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(self, label: str, task: Callable[[], object]) -> None:
        self._queue.put((label, task))

    def shutdown(self) -> None:
        self._stop_event.set()
        self._queue.put(("__STOP__", lambda: None))
        self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                label, task = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if label == "__STOP__":
                break
            try:
                result = task()
            except Exception as exc:  # pragma: no cover - defensive guard
                self._callback("error", label, exc, None)
            else:
                self._callback("done", label, None, result)
            finally:
                self._queue.task_done()


class SyncPanel(ttk.Frame):
    """Composite widget providing Sheets sync controls."""

    def __init__(self, master: tk.Misc) -> None:
        super().__init__(master, padding=12)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        self._settings = load_google_sync_settings()
        self._worker = _BackgroundWorker(self._on_worker_event)
        self._busy_task: Optional[str] = None
        self._auto_sync: Optional[AutoSyncController] = None
        self._last_auto_status: Optional[str] = None
        self.status_var = tk.StringVar(value="")
        self._conflict_items: list[dict[str, object]] = []

        self._build_ui()
        self._update_credentials_state()
        self._update_button_states()

        self._auto_sync = AutoSyncController(
            status_callback=self._handle_auto_sync_status,
            log_callback=self._log_from_worker,
            conflict_callback=self._handle_auto_conflicts,
        )
        self._auto_sync.start()

        if sheets_sync.is_api_available() and self._credentials_exist():
            self.after(750, self._initial_health_check)
        elif not sheets_sync.is_api_available():
            warning = dependency_warning()
            self._append_log(warning)

    def apply_theme(self, palette: dict[str, str]) -> None:
        """Apply the current palette to widgets that are not themed automatically."""

        # `ttk` widgets pick up most styling changes automatically via `ttk.Style`.
        # However, native Tk widgets such as the scrolled text control must be
        # themed manually to remain legible in both light and dark modes.
        self.configure(style="TFrame")
        self.log_widget.configure(
            background=palette["frame"],
            foreground=palette["foreground"],
            insertbackground=palette["foreground"],
            selectbackground=palette["selection"],
            selectforeground=palette["selection_text"],
            highlightbackground=palette["border"],
            highlightcolor=palette["border"],
        )
        if hasattr(self, "conflict_list"):
            self.conflict_list.configure(
                background=palette["frame"],
                foreground=palette["foreground"],
                selectbackground=palette["selection"],
                selectforeground=palette["selection_text"],
                highlightbackground=palette["border"],
                highlightcolor=palette["border"],
            )

    # ------------------------------------------------------------------
    # UI construction helpers
    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        self.spreadsheet_var = tk.StringVar(value=self._settings.spreadsheet_id)
        self.worksheet_var = tk.StringVar(value=self._settings.worksheet_title or DEFAULT_WORKSHEET_TITLE)
        self.service_email_var = tk.StringVar(
            value=self._settings.service_account_email or DEFAULT_SERVICE_ACCOUNT_EMAIL
        )
        self.credentials_path_var = tk.StringVar(value=self._settings.credential_path)

        self.status_banner = ttk.Label(
            self,
            textvariable=self.status_var,
            style="Warning.TLabel",
            anchor="center",
            padding=6,
        )
        self.status_banner.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        self.status_banner.grid_remove()

        form = ttk.LabelFrame(self, text="Google Sheets Settings", padding=12)
        form.grid(row=1, column=0, sticky="nsew")
        form.columnconfigure(1, weight=1)

        ttk.Label(form, text="Sheet ID").grid(row=0, column=0, sticky=tk.W, pady=(0, 6))
        self.spreadsheet_entry = ttk.Entry(form, textvariable=self.spreadsheet_var)
        self.spreadsheet_entry.grid(row=0, column=1, sticky="ew", pady=(0, 6))
        self.spreadsheet_entry.bind("<FocusOut>", self._on_spreadsheet_focus_out)

        ttk.Label(form, text="Service account email").grid(row=1, column=0, sticky=tk.W, pady=6)
        ttk.Entry(form, textvariable=self.service_email_var).grid(row=1, column=1, sticky="ew", pady=6)

        ttk.Label(form, text="Worksheet name").grid(row=2, column=0, sticky=tk.W, pady=6)
        self.worksheet_entry = ttk.Entry(form, textvariable=self.worksheet_var)
        self.worksheet_entry.grid(row=2, column=1, sticky="ew", pady=6)
        self.worksheet_entry.bind("<FocusOut>", self._on_worksheet_focus_out)

        ttk.Label(form, text="Service Account JSON").grid(row=3, column=0, sticky=tk.W, pady=6)
        credentials_frame = ttk.Frame(form)
        credentials_frame.grid(row=3, column=1, sticky="ew", pady=6)
        credentials_frame.columnconfigure(1, weight=1)

        self.choose_button = ttk.Button(
            credentials_frame,
            text="Choose Service Account JSON",
            command=self._on_choose_credentials,
        )
        self.choose_button.grid(row=0, column=0, padx=(0, 6))
        self.credentials_status_var = tk.StringVar()
        ttk.Label(
            credentials_frame,
            textvariable=self.credentials_status_var,
            style="CardHint.TLabel",
        ).grid(row=0, column=1, sticky="w")

        button_frame = ttk.Frame(form)
        button_frame.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        button_frame.columnconfigure(3, weight=1)

        self.push_button = ttk.Button(
            button_frame,
            text="Full Push",
            command=self._on_full_push,
        )
        self.push_button.grid(row=0, column=0, padx=(0, 6))

        self.pull_button = ttk.Button(
            button_frame,
            text="Pull Updates",
            command=self._on_pull_updates,
        )
        self.pull_button.grid(row=0, column=1, padx=(0, 6))

        self.health_button = ttk.Button(
            button_frame,
            text="Health Check",
            command=self._on_health_check,
        )
        self.health_button.grid(row=0, column=2, padx=(0, 6))

        self.logs_button = ttk.Button(
            button_frame,
            text="Open Logs",
            command=self._on_open_logs,
        )
        self.logs_button.grid(row=0, column=3, sticky=tk.W)

        log_frame = ttk.LabelFrame(self, text="Status Log", padding=12)
        log_frame.grid(row=2, column=0, sticky="nsew", pady=(12, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_widget = scrolledtext.ScrolledText(log_frame, width=80, height=16, state="disabled")
        self.log_widget.grid(row=0, column=0, sticky="nsew")

        conflict_frame = ttk.LabelFrame(self, text="Conflicts", padding=12)
        conflict_frame.grid(row=3, column=0, sticky="ew", pady=(12, 0))
        conflict_frame.columnconfigure(0, weight=1)

        self.conflict_list = tk.Listbox(conflict_frame, height=4)
        self.conflict_list.grid(row=0, column=0, sticky="ew")

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------
    def _on_spreadsheet_focus_out(self, _event: tk.Event) -> None:
        parsed = sheets_sync.parse_spreadsheet_id(self.spreadsheet_var.get())
        if parsed and parsed != self.spreadsheet_var.get():
            self.spreadsheet_var.set(parsed)
        self._persist_settings()

    def _on_worksheet_focus_out(self, _event: tk.Event) -> None:
        title = (self.worksheet_var.get() or "").strip()
        if not title:
            title = DEFAULT_WORKSHEET_TITLE
        if title != self.worksheet_var.get():
            self.worksheet_var.set(title)
        if title != (self._settings.worksheet_title or ""):
            self._settings.sheet_gid = ""
        self._persist_settings()

    def _on_choose_credentials(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Select credentials file",
            filetypes=[("JSON Files", "*.json"), ("All Files", "*.*")],
        )
        if not file_path:
            return

        try:
            payload = load_service_account_data(Path(file_path))
        except CredentialsFileInvalidError as exc:
            messagebox.showerror(
                "Credentials File",
                f"CredentialsFileInvalidError: {exc}",
                parent=self.winfo_toplevel(),
            )
            return
        except Exception as exc:  # pragma: no cover - file access guard
            messagebox.showerror(
                "Credentials File",
                f"Credentials file could not be read: {exc}",
                parent=self.winfo_toplevel(),
            )
            return

        target_path = Path(DEFAULT_CREDENTIALS_PATH)
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with target_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
        except OSError as exc:
            messagebox.showerror(
                "Credentials File",
                f"Credentials file could not be saved: {exc}",
                parent=self.winfo_toplevel(),
            )
            return

        client_email = payload.get("client_email")
        if isinstance(client_email, str) and client_email.strip():
            self.service_email_var.set(client_email.strip())
            self._settings.service_account_email = client_email.strip()

        self.credentials_path_var.set(str(target_path))
        self._settings.credential_path = str(target_path)
        self._persist_settings()
        self._update_credentials_state()
        self._append_log(f"Credentials file saved to {target_path}.")
        self._update_button_states()

    def _on_full_push(self) -> None:
        self._enqueue_task("push", self._task_push)

    def _on_pull_updates(self) -> None:
        self._enqueue_task("pull", self._task_pull)

    def _on_health_check(self) -> None:
        self._enqueue_task("health", self._task_health)

    def _on_open_logs(self) -> None:
        self._enqueue_task("logs", self._task_open_logs)

    # ------------------------------------------------------------------
    # Worker integration
    # ------------------------------------------------------------------
    def _enqueue_task(self, label: str, task_callable: Callable[[], object]) -> None:
        if self._busy_task:
            self._append_log("Wait until the current task finishes.")
            return
        if label != "logs" and not self._credentials_exist():
            messagebox.showinfo(
                "Google Sheets",
                "Please select the credentials file first.",
                parent=self.winfo_toplevel(),
            )
            return
        self._busy_task = label
        self._update_button_states()
        self._worker.submit(label, task_callable)

    def _on_worker_event(
        self,
        status: str,
        label: str,
        error: Optional[Exception],
        result: Optional[object],
    ) -> None:
        if status == "error" and error is not None:
            self.after(0, self._handle_task_error, label, error)
        else:
            self.after(0, self._handle_task_result, label, result)

    def _handle_task_error(self, label: str, error: Exception) -> None:
        self._busy_task = None
        self._update_button_states()
        prefix = error.__class__.__name__
        message = str(error)
        if message and not message.startswith(prefix):
            message = f"{prefix}: {message}"
        elif not message:
            message = prefix
        self._append_log(f"Task {label} failed: {message}")

    def _handle_task_result(self, label: str, result: Optional[object]) -> None:
        self._busy_task = None
        self._update_button_states()
        if isinstance(result, dict):
            if label == "push":
                self._append_log(
                    "Full Push completed: "
                    f"{result.get('new', 0)} new, {result.get('changed', 0)} updates."
                )
            elif label == "pull":
                self._append_log(
                    "Pull Updates completed: "
                    f"{result.get('applied', 0)} rows applied (total {result.get('total_remote', 0)})."
                )
            elif label == "health":
                resolved_title = result.get("resolved_title")
                if isinstance(resolved_title, str) and resolved_title:
                    self.worksheet_var.set(resolved_title)
                    self._settings.worksheet_title = resolved_title
                worksheet_id = result.get("worksheet_id")
                if worksheet_id is not None:
                    self._settings.sheet_gid = str(worksheet_id)
                self._persist_settings()
                details = ", ".join(
                    f"{key}={value}" for key, value in sorted(result.items())
                )
                self._append_log(f"Health Check completed: {details}")
        elif isinstance(result, str) and label == "logs":
            self._append_log(f"Log file opened: {result}")

    # ------------------------------------------------------------------
    # Task implementations
    # ------------------------------------------------------------------
    def _task_push(self) -> object:
        settings = self._collect_settings()
        return sheets_sync.push(
            settings.spreadsheet_id,
            settings.credential_path,
            worksheet_title=settings.worksheet_title,
            log_callback=self._log_from_worker,
        )

    def _task_pull(self) -> object:
        settings = self._collect_settings()
        return sheets_sync.pull(
            settings.spreadsheet_id,
            settings.credential_path,
            worksheet_title=settings.worksheet_title,
            log_callback=self._log_from_worker,
        )

    def _task_health(self) -> object:
        settings = self._collect_settings()
        return sheets_sync.health_check(
            settings.spreadsheet_id,
            settings.credential_path,
            worksheet_title=settings.worksheet_title,
            sheet_gid=settings.sheet_gid,
            service_account_email=settings.service_account_email,
        )

    def _task_open_logs(self) -> object:
        path = sheets_sync.open_logs()
        try:
            if sys.platform.startswith("win"):
                os_startfile = getattr(os, "startfile", None)
                if os_startfile:
                    os_startfile(path)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", path])
            else:
                subprocess.Popen(["xdg-open", path])
        except Exception:  # pragma: no cover - system dependent
            pass
        return path

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    def _collect_settings(self) -> GoogleSyncSettings:
        spreadsheet_id = sheets_sync.parse_spreadsheet_id(self.spreadsheet_var.get())
        worksheet_title = (self.worksheet_var.get() or DEFAULT_WORKSHEET_TITLE).strip()
        credential_path = self.credentials_path_var.get() or DEFAULT_CREDENTIALS_PATH
        service_email = self.service_email_var.get() or DEFAULT_SERVICE_ACCOUNT_EMAIL
        return GoogleSyncSettings(
            spreadsheet_id=spreadsheet_id or DEFAULT_SPREADSHEET_ID,
            credential_path=credential_path,
            service_account_email=service_email,
            worksheet_title=worksheet_title or DEFAULT_WORKSHEET_TITLE,
            sheet_gid=self._settings.sheet_gid,
        )

    def _persist_settings(self) -> None:
        self._settings.spreadsheet_id = self.spreadsheet_var.get()
        self._settings.worksheet_title = self.worksheet_var.get()
        self._settings.service_account_email = self.service_email_var.get()
        self._settings.credential_path = self.credentials_path_var.get()
        save_google_sync_settings(self._settings)

    def _credentials_exist(self) -> bool:
        path = Path(self.credentials_path_var.get() or DEFAULT_CREDENTIALS_PATH)
        return path.exists()

    def _update_credentials_state(self) -> None:
        if self._credentials_exist():
            self.credentials_status_var.set("Available")
        else:
            self.credentials_status_var.set("Missing")

    def _update_button_states(self) -> None:
        disabled = bool(self._busy_task)
        for widget in (self.push_button, self.pull_button, self.health_button, self.logs_button):
            if disabled and widget is not self.logs_button:
                widget.state(["disabled"])
            else:
                widget.state(["!disabled"])

    def _append_log(self, message: str) -> None:
        self.log_widget.configure(state="normal")
        self.log_widget.insert(tk.END, message + "\n")
        self.log_widget.configure(state="disabled")
        self.log_widget.see(tk.END)

    def _log_from_worker(self, message: str) -> None:
        self.after(0, self._append_log, message)

    def _handle_auto_sync_status(self, status: str, payload: dict[str, object]) -> None:
        self.after(0, self._update_auto_status, status, payload)

    def _update_auto_status(self, status: str, payload: dict[str, object]) -> None:
        reason = payload.get("reason") if isinstance(payload, dict) else None
        if status == "disabled" and reason == "dependencies":
            self.status_var.set("Sync disabled: dependencies missing")
            self.status_banner.grid()
        else:
            self.status_var.set("")
            self.status_banner.grid_remove()

        if status == "synced":
            pull_stats = payload.get("pull") if isinstance(payload, dict) else None
            push_stats = payload.get("push") if isinstance(payload, dict) else None
            pull_applied = (
                int(pull_stats.get("applied", 0)) if isinstance(pull_stats, dict) else 0
            )
            push_total = (
                int(push_stats.get("total", 0)) if isinstance(push_stats, dict) else 0
            )
            if pull_applied or push_total:
                self._append_log(
                    f"Automatic sync completed: pull={pull_applied}, push={push_total}."
                )
        elif status == "offline" and status != self._last_auto_status:
            message = payload.get("message") if isinstance(payload, dict) else None
            detail = message or "Google Sheets unreachable"
            self._append_log(f"Automatic sync offline: {detail}")
        elif status == "error" and status != self._last_auto_status:
            message = payload.get("message") if isinstance(payload, dict) else None
            detail = message or "unexpected condition"
            self._append_log(f"Automatic sync error: {detail}")

        self._last_auto_status = status

    def _handle_auto_conflicts(self, payload: dict[str, object]) -> None:
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            items = []
        self.after(0, self._update_conflicts, items)

    def _update_conflicts(self, items: list[dict[str, object]]) -> None:
        self._conflict_items = items
        if hasattr(self, "conflict_list"):
            self.conflict_list.delete(0, tk.END)
            for entry in items:
                row_id = str(entry.get("row_id", ""))
                fields = entry.get("fields") if isinstance(entry, dict) else {}
                if isinstance(fields, dict):
                    field_names = ", ".join(sorted(fields.keys()))
                else:
                    field_names = ""
                timestamp = str(entry.get("timestamp", ""))
                summary = f"{timestamp} · {row_id}"
                if field_names:
                    summary += f" ({field_names})"
                self.conflict_list.insert(tk.END, summary)

    def _initial_health_check(self) -> None:
        self._append_log("Verifying connectivity and permissions...")
        self._enqueue_task("health", self._task_health)

    def destroy(self) -> None:  # pragma: no cover - Tkinter lifecycle
        if self._auto_sync:
            self._auto_sync.stop()
        self._worker.shutdown()
        super().destroy()


__all__ = ["SyncPanel"]
