"""Minimal synchronisation settings dialog for RugBase."""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core import sheets_gateway
from settings import DEFAULT_SPREADSHEET_ID, load_google_sync_settings, save_google_sync_settings


class SyncSettingsWindow:
    """Dialog that lets the user configure the Google Sheet identifier."""

    def __init__(self, master: tk.Misc) -> None:
        self.window = tk.Toplevel(master)
        self.window.title("Sync Settings")
        self.window.transient(master)
        self.window.resizable(False, False)

        self._settings = load_google_sync_settings()
        sheet_id = self._settings.spreadsheet_id or DEFAULT_SPREADSHEET_ID
        self.sheet_id_var = tk.StringVar(value=sheet_id)

        self._build_ui()

    # ------------------------------------------------------------------
    # UI helpers
    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        frame = ttk.Frame(self.window, padding=12)
        frame.grid(row=0, column=0, sticky="nsew")

        ttk.Label(frame, text="Google Sheet ID:").grid(row=0, column=0, sticky="w")
        entry = ttk.Entry(frame, textvariable=self.sheet_id_var, width=48)
        entry.grid(row=1, column=0, sticky="ew", pady=(4, 12))
        entry.bind("<FocusOut>", lambda _event: self._persist())

        button_bar = ttk.Frame(frame)
        button_bar.grid(row=2, column=0, sticky="ew")
        button_bar.columnconfigure(1, weight=1)

        ttk.Button(button_bar, text="Test Connection", command=self._on_test_connection).grid(
            row=0, column=0, padx=(0, 8)
        )
        ttk.Button(button_bar, text="Close", command=self.window.destroy).grid(row=0, column=2)

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------
    def _persist(self) -> None:
        sheet_id = self.sheet_id_var.get().strip()
        if not sheet_id:
            return
        self._settings.spreadsheet_id = sheet_id
        save_google_sync_settings(self._settings)

    def _on_test_connection(self) -> None:
        sheet_id = self.sheet_id_var.get().strip()
        if not sheet_id:
            messagebox.showwarning("Sync Settings", "Please enter a Sheet ID.", parent=self.window)
            return

        try:
            sheets_gateway.get_rows(spreadsheet_id=sheet_id)
        except sheets_gateway.CredentialsNotFoundError as exc:
            messagebox.showerror(
                "Sync Settings",
                f"Service account not found:\n{exc}",
                parent=self.window,
            )
        except sheets_gateway.StatusValidationError as exc:
            # Status validation only triggers on upload; treat as generic error for connection tests.
            messagebox.showerror("Sync Settings", str(exc), parent=self.window)
        except sheets_gateway.MissingDependencyError as exc:
            messagebox.showerror("Sync Settings", str(exc), parent=self.window)
        except sheets_gateway.SheetsGatewayError as exc:
            messagebox.showerror(
                "Sync Settings",
                f"Connection failed: {exc}",
                parent=self.window,
            )
        except Exception as exc:  # pragma: no cover - defensive fallback
            messagebox.showerror("Sync Settings", str(exc), parent=self.window)
        else:
            messagebox.showinfo("Sync Settings", "Connection successful.", parent=self.window)


__all__ = ["SyncSettingsWindow"]

