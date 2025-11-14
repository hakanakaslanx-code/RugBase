import csv
import logging
import os
import subprocess
import sys
import threading
import webbrowser
from datetime import datetime
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter import font as tkfont
from typing import Any, Callable, Dict, List, Optional, Sequence
import shutil
from pathlib import Path

import db
from core import app_paths, importer, updater
from core.inventory_sync import InventoryStatus, InventorySyncManager
from core.logging_config import get_log_path
from core.version import __version__
from ui_item_card import ItemCardWindow
from ui_label_generator import LabelGeneratorWindow
from core.excel import Workbook
from ttkbootstrap import Style
from settings import DEFAULT_CREDENTIALS_PATH, load_google_sync_settings, save_google_sync_settings
from core.google_credentials import CredentialsFileInvalidError, ensure_service_account_file
from core.sheets_client import SheetsClientError
from ui.sync_settings import SyncSettingsWindow

logger = logging.getLogger(__name__)


class ScrollableFrame(ttk.Frame):
    """A simple vertically scrollable container for notebook pages."""

    def __init__(self, master: tk.Misc, *, padding: int = 0) -> None:
        super().__init__(master, padding=padding)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self._canvas = tk.Canvas(self, borderwidth=0, highlightthickness=0)
        self._canvas.grid(row=0, column=0, sticky="nsew")

        self._scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self._canvas.yview)
        self._scrollbar.grid(row=0, column=1, sticky="ns")

        self._canvas.configure(yscrollcommand=self._scrollbar.set)

        self.content = ttk.Frame(self._canvas, padding=padding)
        self.content.columnconfigure(0, weight=1)

        self._window_id = self._canvas.create_window((0, 0), window=self.content, anchor="nw")

        self.content.bind("<Configure>", self._on_content_configure)
        self._canvas.bind("<Configure>", self._on_canvas_configure)

        for sequence in ("<MouseWheel>", "<Button-4>", "<Button-5>"):
            self.content.bind(sequence, self._on_mousewheel, add=True)
            self._canvas.bind(sequence, self._on_mousewheel, add=True)

    def _on_content_configure(self, event: tk.Event) -> None:
        self._canvas.configure(scrollregion=self._canvas.bbox("all"))

    def _on_canvas_configure(self, event: tk.Event) -> None:
        self._canvas.itemconfigure(self._window_id, width=event.width)

    def _on_mousewheel(self, event: tk.Event) -> None:
        if event.delta:
            self._canvas.yview_scroll(int(-event.delta / 120), "units")
        elif getattr(event, "num", None) == 4:
            self._canvas.yview_scroll(-1, "units")
        elif getattr(event, "num", None) == 5:
            self._canvas.yview_scroll(1, "units")


class MainWindow:
    def __init__(
        self,
        root: tk.Tk,
        column_changes: Optional[List[str]] = None,
        *,
        initial_online: bool = True,
        initial_error: Optional[str] = None,
        force_sync_settings: bool = False,
    ) -> None:
        self.root = root
        self.label_window: Optional[LabelGeneratorWindow] = None
        self.current_user = os.getenv("USERNAME") or os.getenv("USER") or "operator"
        # Initialize ttkbootstrap styling for the current Tk root window.  Newer
        # releases of ttkbootstrap no longer accept a ``master`` keyword
        # argument, so simply instantiating ``Style`` after creating the root
        # window ensures that the existing root is reused.
        self.style = Style(theme="flatly")
        self.dark_mode_var = tk.BooleanVar(value=False)
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", self._on_search_change)
        self.show_sold_var = tk.BooleanVar(value=False)
        self.sync_state_var = tk.StringVar(value="Checking connection…")
        self.user_status_var = tk.StringVar()
        self.last_sync_var = tk.StringVar(value="—")
        self.sync_status_badge_var = tk.StringVar(value="Checking connection…")
        self.sync_detail_var = tk.StringVar(value="Verifying Google Sheets connection…")
        self.sync_pending_var = tk.StringVar(value="Pending changes: 0")
        self.summary_var = tk.StringVar(
            value="Total Items: 0 | Total Area: 0.00 sq ft | Last Sync: —"
        )
        self.auto_save_var = tk.BooleanVar(value=False)
        self.activity_log_entries: List[str] = [
            "Updated Rug 48605",
            "Exported CSV",
            "Printed Label",
        ]
        self.activity_log_var = tk.StringVar(value=self._format_activity_log())
        self.active_view = "dashboard"
        self.nav_labels: Dict[str, tk.Label] = {}
        self.tile_buttons: List[Dict[str, Any]] = []
        self._sidebar_base_color = "#f8f8f8"
        self._last_loaded_items: List[Dict[str, Any]] = []
        self._sidebar_hover_color = "#e4e8ef"
        self._sidebar_active_color = "#0078D7"
        self._sidebar_fg = "#1f2933"
        self._sidebar_active_fg = "#ffffff"
        self._palette: dict[str, str] = {}
        self.customer_form_vars: Dict[str, tk.StringVar] = {}
        self.customer_notes_text: Optional[tk.Text] = None
        self.customer_tree: Optional[ttk.Treeview] = None
        self.customer_records: Dict[str, Dict[str, Any]] = {}
        self.sales_period_days: Optional[int] = 30
        self.column_status_var = tk.StringVar(value="")
        self._startup_column_changes = list(column_changes or [])
        self._auto_save_job: Optional[str] = None
        self._offline_controls: List[tk.Widget] = []
        self._offline = not initial_online
        self._sync_detail_default = self.sync_detail_var.get()
        self._conflict_toast_job: Optional[str] = None
        self.sync_manager = InventorySyncManager(status_callback=self._handle_sync_status)
        self._initial_error = initial_error
        self._force_sync_settings = force_sync_settings
        self._sync_settings_window: Optional[SyncSettingsWindow] = None
        self._configure_style()
        self._create_widgets()
        self._apply_offline_state(not initial_online)
        self.load_items()
        self.load_customers()
        self.root.bind("<Control-l>", self.on_open_label_generator)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.minsize(1024, 640)
        self._apply_theme()
        self._maybe_show_startup_column_changes()
        self._update_user_status()
        self._start_sync_workers()
        self._update_connection_view()
        if self._force_sync_settings:
            self.root.after(200, self._open_sync_settings_window)

    def _start_sync_workers(self) -> None:
        def _bootstrap() -> None:
            online = self.sync_manager.load_initial_snapshot()
            self.root.after(0, lambda: self._after_initial_sync(online))

        threading.Thread(target=_bootstrap, daemon=True).start()
        self.sync_manager.start()

    def _after_initial_sync(self, online: bool) -> None:
        self._apply_offline_state(not online)
        self.load_items()
        self.load_customers()
        self._update_connection_view()

    def _apply_offline_state(self, offline: bool) -> None:
        self._offline = offline
        state = tk.DISABLED if offline else tk.NORMAL
        for widget in self._offline_controls:
            try:
                widget.configure(state=state)
            except tk.TclError:
                continue
        self._update_sync_badge(not offline)

    def _update_sync_badge(self, online: bool) -> None:
        if not hasattr(self, "sync_badge"):
            return
        background = "#0f5132" if online else "#b91c1c"
        self.sync_badge.configure(background=background, foreground="#ffffff")

    def _handle_sync_status(self, status: InventoryStatus) -> None:
        self.root.after(0, lambda: self._apply_sync_status(status))

    def _apply_sync_status(self, status: InventoryStatus) -> None:
        online = status.online
        if online:
            badge_text = "ONLINE – syncing" if status.pending else "ONLINE – synced"
            detail = (
                f"Last sync: {status.last_sync}" if status.last_sync else "Last sync: —"
            )
        else:
            badge_text = "OFFLINE – read-only"
            detail = status.error or "Connection unavailable. Changes will be queued."

        if status.message:
            detail = status.message

        self.sync_status_badge_var.set(badge_text)
        self.sync_state_var.set(badge_text)
        self._sync_detail_default = detail
        if status.error and not online:
            self.sync_detail_var.set(status.error)
        else:
            self.sync_detail_var.set(detail)
        pending_text = (
            f"Pending changes: {status.pending}"
            if status.pending
            else "Pending changes: 0"
        )
        self.sync_pending_var.set(pending_text)
        self.last_sync_var.set(status.last_sync or "—")
        self._apply_offline_state(not online)
        self._update_user_status()
        self._update_connection_view()
        if status.conflicts:
            for rug_no in status.conflicts:
                self._show_conflict_message(rug_no)

    def _show_conflict_message(self, rug_no: str) -> None:
        message = f"Row {rug_no} was updated remotely"
        self.sync_detail_var.set(message)
        if self._conflict_toast_job:
            self.root.after_cancel(self._conflict_toast_job)
        self._conflict_toast_job = self.root.after(6000, self._restore_sync_detail)

    def _restore_sync_detail(self) -> None:
        self.sync_detail_var.set(self._sync_detail_default)
        self._conflict_toast_job = None

    def on_sync_now(self) -> None:
        self.sync_manager.sync_now()
        self._sync_detail_default = "Synchronization started…"
        self.sync_detail_var.set(self._sync_detail_default)

    def on_open_sheet(self) -> None:
        settings = load_google_sync_settings()
        sheet_id = settings.spreadsheet_id
        if not sheet_id:
            messagebox.showinfo(
                "Google Sheets",
                "Sheet ID is not configured in settings.",
                parent=self.root,
            )
            return
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        try:
            webbrowser.open(url)
        except Exception as exc:
            messagebox.showerror(
                "Google Sheets", f"Could not open spreadsheet: {exc}", parent=self.root
            )

    def _configure_style(self) -> None:
        default_font = tkfont.nametofont("TkDefaultFont")
        default_font.configure(family="Segoe UI", size=10)

        text_font = tkfont.nametofont("TkTextFont")
        text_font.configure(family="Segoe UI", size=10)

        menu_font = tkfont.nametofont("TkMenuFont")
        menu_font.configure(family="Segoe UI", size=10)

        self.style.configure(".", font=default_font)
        self.style.configure("TButton", font=default_font)
        self.style.configure("TLabel", font=default_font)
        self.style.configure("Header.TLabel", font=("Segoe UI", 18, "bold"))
        self.style.configure("Hero.TLabel", font=("Segoe UI", 24, "bold"))
        self.style.configure("SubHeader.TLabel", font=("Segoe UI", 10))
        self.style.configure("Accent.TButton", font=("Segoe UI", 10, "bold"))
        self.style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))
        self.style.configure("Card.TFrame", relief="flat", borderwidth=0, padding=16)
        self.style.configure("TileTitle.TLabel", font=("Segoe UI", 14, "bold"), foreground="#ffffff")
        self.style.configure("TileSubtitle.TLabel", font=("Segoe UI", 10), foreground="#f4f4f4")
        self.style.configure("InfoBadge.TLabel", font=("Segoe UI", 10, "bold"))

    def _light_palette(self) -> dict[str, str]:
        return {
            "background": "#f5f7fb",
            "frame": "#ffffff",
            "foreground": "#1f2933",
            "subtext": "#4b5563",
            "accent": "#2563eb",
            "accent_text": "#ffffff",
            "accent_hover": "#3b82f6",
            "accent_pressed": "#1d4ed8",
            "selection": "#2563eb",
            "selection_text": "#ffffff",
            "tree_background": "#ffffff",
            "border": "#d1d5db",
            "button_hover": "#e5e7eb",
            "button_pressed": "#d1d5db",
            "entry": "#ffffff",
            "disabled_bg": "#e5e7eb",
            "disabled_fg": "#9ca3af",
            "warning": "#b91c1c",
        }

    def _dark_palette(self) -> dict[str, str]:
        return {
            "background": "#1f1f24",
            "frame": "#2b2f3a",
            "foreground": "#f3f4f6",
            "subtext": "#9ca3af",
            "accent": "#3b82f6",
            "accent_text": "#f8fafc",
            "accent_hover": "#60a5fa",
            "accent_pressed": "#2563eb",
            "selection": "#3b82f6",
            "selection_text": "#f8fafc",
            "tree_background": "#1f2937",
            "border": "#3f3f46",
            "button_hover": "#3f3f46",
            "button_pressed": "#1f2933",
            "entry": "#111827",
            "disabled_bg": "#2f313d",
            "disabled_fg": "#6b7280",
            "warning": "#f87171",
        }

    def _toggle_dark_mode(self) -> None:
        self._apply_theme()

    def _apply_theme(self) -> None:
        palette = self._dark_palette() if self.dark_mode_var.get() else self._light_palette()
        self._palette = palette

        self.root.configure(bg=palette["background"])

        self.style.configure("TFrame", background=palette["background"])
        self.style.configure(
            "TLabelframe",
            background=palette["frame"],
            bordercolor=palette["border"],
            relief="solid",
        )
        self.style.configure(
            "TLabelframe.Label",
            background=palette["frame"],
            foreground=palette["foreground"],
        )
        self.style.configure("TLabel", background=palette["background"], foreground=palette["foreground"])
        self.style.configure("Header.TLabel", background=palette["background"], foreground=palette["foreground"])
        self.style.configure("SubHeader.TLabel", background=palette["background"], foreground=palette["subtext"])
        self.style.configure("Hint.TLabel", background=palette["background"], foreground=palette["subtext"])
        self.style.configure("CardHint.TLabel", background=palette["frame"], foreground=palette["subtext"])
        self.style.configure("Info.TLabel", background=palette["frame"], foreground=palette["accent"])
        self.style.configure("Warning.TLabel", background=palette["frame"], foreground=palette["warning"])
        self.style.configure("TNotebook", background=palette["background"], bordercolor=palette["border"])
        self.style.configure(
            "TNotebook.Tab",
            background=palette["frame"],
            foreground=palette["subtext"],
            bordercolor=palette["border"],
        )
        self.style.map(
            "TNotebook.Tab",
            background=[("selected", palette["background"])],
            foreground=[("selected", palette["foreground"])],
        )
        self.style.configure(
            "Treeview",
            background=palette["tree_background"],
            fieldbackground=palette["tree_background"],
            foreground=palette["foreground"],
            bordercolor=palette["border"],
            rowheight=24,
        )
        self.style.configure(
            "Treeview.Heading",
            background=palette["frame"],
            foreground=palette["foreground"],
            bordercolor=palette["border"],
        )
        self.style.map(
            "Treeview",
            background=[("selected", palette["selection"])],
            foreground=[("selected", palette["selection_text"])],
        )
        self.style.configure(
            "TButton",
            background=palette["frame"],
            foreground=palette["foreground"],
            bordercolor=palette["border"],
        )
        self.style.map(
            "TButton",
            background=[("active", palette["button_hover"]), ("pressed", palette["button_pressed"])],
        )
        self.style.configure(
            "Accent.TButton",
            background=palette["accent"],
            foreground=palette["accent_text"],
            bordercolor=palette["accent_pressed"],
        )
        self.style.map(
            "Accent.TButton",
            background=[("active", palette["accent_hover"]), ("pressed", palette["accent_pressed"])],
        )
        self.style.configure(
            "TEntry",
            fieldbackground=palette["entry"],
            foreground=palette["foreground"],
            bordercolor=palette["border"],
        )
        self.style.map(
            "TEntry",
            fieldbackground=[("disabled", palette["disabled_bg"])],
            foreground=[("disabled", palette["disabled_fg"])],
        )
        self.style.configure(
            "TCombobox",
            fieldbackground=palette["entry"],
            foreground=palette["foreground"],
            bordercolor=palette["border"],
            arrowcolor=palette["foreground"],
        )
        self.style.map(
            "TCombobox",
            fieldbackground=[("readonly", palette["entry"])],
            foreground=[("readonly", palette["foreground"])],
        )
        self.style.configure(
            "Vertical.TScrollbar",
            background=palette["frame"],
            troughcolor=palette["background"],
            bordercolor=palette["border"],
        )
        self.style.configure(
            "Horizontal.TScrollbar",
            background=palette["frame"],
            troughcolor=palette["background"],
            bordercolor=palette["border"],
        )

        if hasattr(self, "sidebar_frame"):
            self.sidebar_frame.configure(bg=self._sidebar_base_color)
            for widget in self.nav_labels.values():
                widget.configure(bg=self._sidebar_base_color, fg=self._sidebar_fg, font=("Segoe UI", 11))
            self._update_nav_state()

        for tile_info in getattr(self, "tile_buttons", []):
            outer = tile_info["outer"]
            shadow = tile_info["shadow"]
            tile = tile_info["tile"]
            base_color = tile_info["color"]
            title = tile_info["title"]
            desc = tile_info["description"]
            outer.configure(bg=palette["background"])
            shadow.configure(bg=self._lighten_color(base_color, 0.35))
            tile.configure(bg=base_color)
            title.configure(bg=base_color)
            desc.configure(bg=base_color)

        if hasattr(self, "dashboard_container"):
            self.dashboard_container._canvas.configure(
                background=palette["background"],
                highlightbackground=palette["border"],
            )

        if self.label_window and self.label_window.window.winfo_exists():
            self.label_window.apply_theme(palette)

    def _maybe_show_startup_column_changes(self) -> None:
        if not self._startup_column_changes:
            return

        def _notify() -> None:
            message = self._format_column_check_message(self._startup_column_changes)
            messagebox.showinfo("Inventory Columns", message)
            self.column_status_var.set(message)
        self.log_activity("Column audit complete")

        self.root.after(200, _notify)

    @staticmethod
    def _format_column_check_message(columns: Sequence[str]) -> str:
        formatted = ", ".join(sorted(columns))
        return f"New columns added: {formatted}"

    def _create_widgets(self) -> None:
        self._build_menu()
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(1, weight=1)

        self.sidebar_canvas = tk.Canvas(self.root, width=220, highlightthickness=0, bd=0)
        self.sidebar_canvas.grid(row=0, column=0, sticky="ns")
        self.sidebar_canvas.bind("<Configure>", self._on_sidebar_configure)

        self.sidebar_frame = tk.Frame(self.sidebar_canvas, bd=0, highlightthickness=0)
        self.sidebar_window = self.sidebar_canvas.create_window(
            (0, 0), window=self.sidebar_frame, anchor="nw", width=220
        )

        self._build_sidebar()

        self.content_container = ttk.Frame(self.root)
        self.content_container.grid(row=0, column=1, sticky="nsew")
        self.content_container.grid_rowconfigure(0, weight=1)
        self.content_container.grid_columnconfigure(0, weight=1)

        self.views: Dict[str, ttk.Frame] = {}

        self.dashboard_frame = ttk.Frame(self.content_container, padding=24)
        self.dashboard_frame.grid(row=0, column=0, sticky="nsew")
        self.views["dashboard"] = self.dashboard_frame
        self._build_dashboard_view(self.dashboard_frame)

        self.customers_frame = ttk.Frame(self.content_container, padding=24)
        self.customers_frame.grid(row=0, column=0, sticky="nsew")
        self.views["customers"] = self.customers_frame
        self._build_customers_view(self.customers_frame)

        self.sync_frame = ttk.Frame(self.content_container, padding=24)
        self.sync_frame.grid(row=0, column=0, sticky="nsew")
        self.views["sync"] = self.sync_frame
        self._build_sync_view(self.sync_frame)

        self.sales_frame = ttk.Frame(self.content_container, padding=24)
        self.sales_frame.grid(row=0, column=0, sticky="nsew")
        self.views["sales"] = self.sales_frame
        self._build_placeholder_view(
            self.sales_frame,
            "Sales & Consignments",
            "Track consignments, mark rugs as sold, and review revenue snapshots.",
        )

        self.reports_frame = ttk.Frame(self.content_container, padding=24)
        self.reports_frame.grid(row=0, column=0, sticky="nsew")
        self.views["reports"] = self.reports_frame
        self._build_placeholder_view(
            self.reports_frame,
            "Reports",
            "Generate performance insights and export analytics for your collections.",
        )

        self.settings_frame = ttk.Frame(self.content_container, padding=24)
        self.settings_frame.grid(row=0, column=0, sticky="nsew")
        self.views["settings"] = self.settings_frame
        self._build_placeholder_view(
            self.settings_frame,
            "Settings",
            "Configure RugBase, manage automation, and review system health.",
        )

        self._build_connection_overlay()
        self.show_view("dashboard")

    def _build_sidebar(self) -> None:
        self.sidebar_frame.configure(bg=self._sidebar_base_color)

        logo = tk.Label(
            self.sidebar_frame,
            text="RugBase",
            font=("Segoe UI", 18, "bold"),
            bg=self._sidebar_base_color,
            fg=self._sidebar_fg,
            anchor="w",
            padx=24,
            pady=20,
        )
        logo.pack(fill=tk.X)

        ttk.Separator(self.sidebar_frame).pack(fill=tk.X, padx=16, pady=(0, 12))

        nav_items = [
            ("dashboard", "🏠", "Dashboard"),
            ("inventory", "📦", "Inventory"),
            ("customers", "👥", "Customers"),
            ("sales", "💰", "Sales"),
            ("reports", "📊", "Reports"),
            ("settings", "⚙️", "Settings"),
            ("sync", "🔄", "Sync & Backup"),
        ]

        for key, icon, label in nav_items:
            nav_label = tk.Label(
                self.sidebar_frame,
                text=f"{icon}  {label}",
                font=("Segoe UI", 11),
                anchor="w",
                bg=self._sidebar_base_color,
                fg=self._sidebar_fg,
                padx=24,
                pady=10,
            )
            nav_label.pack(fill=tk.X, padx=12, pady=2)
            nav_label.bind(
                "<Button-1>",
                lambda _event, view=key: self.show_view("dashboard" if view == "inventory" else view),
            )
            nav_label.bind(
                "<Enter>",
                lambda _event, widget=nav_label: widget.configure(bg=self._sidebar_hover_color),
            )
            nav_label.bind(
                "<Leave>",
                lambda _event, view=key, widget=nav_label: self._refresh_nav_label(view, widget),
            )
            self.nav_labels[key] = nav_label

        self._update_nav_state()

    def _build_connection_overlay(self) -> None:
        self.connection_overlay = tk.Frame(self.root, background="#f5f6f8")
        inner = ttk.Frame(self.connection_overlay, padding=48, style="Card.TFrame")
        inner.pack(expand=True, padx=40, pady=40)

        ttk.Label(inner, text="Google Sheets connection required", style="Hero.TLabel").pack(
            anchor="center", pady=(0, 12)
        )
        ttk.Label(
            inner,
            text=(
                "RugBase now manages all data through Google Sheets."
                " Upload a valid service account JSON file to continue."
            ),
            wraplength=480,
            justify=tk.CENTER,
        ).pack(anchor="center", pady=(0, 16))

        self.connection_status_var = tk.StringVar(value="Waiting for connection")
        ttk.Label(inner, textvariable=self.connection_status_var, style="Warning.TLabel").pack(
            anchor="center", pady=(0, 16)
        )

        button_row = ttk.Frame(inner)
        button_row.pack(anchor="center")
        ttk.Button(button_row, text="Upload JSON", command=self._on_upload_credentials).pack(
            side=tk.LEFT, padx=(0, 8)
        )
        ttk.Button(button_row, text="Try Again", command=self.on_sync_now).pack(
            side=tk.LEFT, padx=(0, 8)
        )
        ttk.Button(
            button_row,
            text="Configure Settings",
            command=self._open_sync_settings_window,
        ).pack(side=tk.LEFT)

        ttk.Label(
            inner,
            text=f"Target: {DEFAULT_CREDENTIALS_PATH}",
            style="Hint.TLabel",
        ).pack(anchor="center", pady=(16, 0))

        self.connection_overlay.place_forget()

    def _update_connection_view(self) -> None:
        online = db.is_online()
        if online:
            self.connection_overlay.place_forget()
            self._apply_offline_state(False)
            self._sync_detail_default = self.sync_detail_var.get()
            return

        error = self._initial_error or db.last_sync_error()
        if error:
            self.connection_status_var.set(error)
        else:
            self.connection_status_var.set("credentials.json must be uploaded.")
        self.connection_overlay.place(relx=0, rely=0, relwidth=1, relheight=1)
        self.connection_overlay.lift()
        self._apply_offline_state(True)

    def _open_sync_settings_window(self) -> None:
        existing = getattr(self, "_sync_settings_window", None)
        if existing and existing.window.winfo_exists():
            existing.window.lift()
            existing.window.focus_force()
            return
        self._sync_settings_window = SyncSettingsWindow(self.root)
        self._sync_settings_window.window.protocol("WM_DELETE_WINDOW", self._on_sync_settings_closed)

    def _on_sync_settings_closed(self) -> None:
        if self._sync_settings_window:
            try:
                if self._sync_settings_window.window.winfo_exists():
                    self._sync_settings_window.window.destroy()
            except tk.TclError:
                pass
        self._sync_settings_window = None

    def _on_upload_credentials(self) -> None:
        path = filedialog.askopenfilename(
            title="Select service account JSON",
            filetypes=(("JSON", "*.json"), ("All files", "*.*")),
        )
        if not path:
            return
        source = Path(path)
        try:
            ensure_service_account_file(source)
        except CredentialsFileInvalidError as exc:
            messagebox.showerror("Invalid JSON", str(exc))
            return

        target = Path(DEFAULT_CREDENTIALS_PATH)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(source, target)
            ensure_service_account_file(target)
        except (OSError, CredentialsFileInvalidError) as exc:
            messagebox.showerror("File error", str(exc))
            return

        settings = load_google_sync_settings()
        settings.credential_path = str(target)
        save_google_sync_settings(settings)

        try:
            db.initialize_database()
        except SheetsClientError as exc:
            self._initial_error = str(exc)
            self._update_connection_view()
            messagebox.showerror("Connection error", str(exc))
            return

        self._initial_error = None
        self.sync_manager.sync_now()
        self.load_items()
        self.load_customers()
        self._update_connection_view()
        messagebox.showinfo("Success", "Google Sheets connection updated.")

    def _draw_vertical_gradient(
        self, canvas: tk.Canvas, width: int, height: int, start_color: str, end_color: str
    ) -> None:
        canvas.delete("gradient")
        if height <= 0:
            return
        r1, g1, b1 = (value // 256 for value in self.root.winfo_rgb(start_color))
        r2, g2, b2 = (value // 256 for value in self.root.winfo_rgb(end_color))
        steps = max(1, min(height, 120))
        for index in range(steps):
            ratio = index / max(steps - 1, 1)
            r = int(r1 + (r2 - r1) * ratio)
            g = int(g1 + (g2 - g1) * ratio)
            b = int(b1 + (b2 - b1) * ratio)
            color = f"#{r:02x}{g:02x}{b:02x}"
            y0 = int(height * index / steps)
            y1 = int(height * (index + 1) / steps)
            canvas.create_rectangle(0, y0, width, y1, fill=color, outline="", tags="gradient")

    def _on_sidebar_configure(self, event: tk.Event) -> None:
        self._draw_vertical_gradient(
            self.sidebar_canvas, event.width, event.height, "#f8f8f8", "#e8e8e8"
        )
        self.sidebar_canvas.configure(scrollregion=self.sidebar_canvas.bbox("all"))

    def _refresh_nav_label(self, view: str, widget: tk.Label) -> None:
        if view == self.active_view or (view == "inventory" and self.active_view == "dashboard"):
            widget.configure(bg=self._sidebar_active_color, fg=self._sidebar_active_fg)
        else:
            widget.configure(bg=self._sidebar_base_color, fg=self._sidebar_fg)

    def _update_nav_state(self) -> None:
        for key, widget in self.nav_labels.items():
            effective_key = "dashboard" if key == "inventory" else key
            self._refresh_nav_label(key, widget)
            if effective_key == self.active_view:
                widget.configure(bg=self._sidebar_active_color, fg=self._sidebar_active_fg)

    def show_view(self, view: str) -> None:
        if view not in self.views and view != "inventory":
            return
        if view == "inventory":
            view = "dashboard"
        self.active_view = view
        frame = self.views.get(view)
        if frame:
            frame.tkraise()
        self._update_nav_state()

    def _create_tile(
        self,
        parent: tk.Misc,
        title: str,
        color: str,
        description: str,
        command: Callable[[], None],
    ) -> tk.Frame:
        outer = tk.Frame(parent, bg=self._palette.get("background", "#f5f7fb"))
        shadow_color = self._lighten_color(color, 0.35)
        shadow = tk.Frame(outer, bg=shadow_color, bd=0, highlightthickness=0)
        shadow.pack(fill=tk.BOTH, expand=True, padx=(6, 2), pady=(8, 2))

        tile = tk.Frame(shadow, bg=color, bd=0, highlightthickness=0)
        tile.pack(fill=tk.BOTH, expand=True)
        tile.configure(padx=18, pady=18)

        title_label = tk.Label(tile, text=title, font=("Segoe UI", 14, "bold"), bg=color, fg="#ffffff")
        title_label.pack(anchor="w")
        description_label = tk.Label(
            tile,
            text=description,
            bg=color,
            fg="#f5f5f5",
            font=("Segoe UI", 10),
            justify=tk.LEFT,
            wraplength=220,
        )
        description_label.pack(anchor="w", pady=(8, 0))

        hover_color = self._lighten_color(color, 0.14)

        def _on_enter(_event: tk.Event) -> None:
            tile.configure(bg=hover_color, padx=22, pady=22)
            title_label.configure(bg=hover_color)
            description_label.configure(bg=hover_color)
            tile.configure(cursor="hand2")

        def _on_leave(_event: tk.Event) -> None:
            tile.configure(bg=color, padx=18, pady=18)
            title_label.configure(bg=color)
            description_label.configure(bg=color)
            tile.configure(cursor="")

        def _on_click(_event: tk.Event) -> None:
            command()

        for widget in (tile, title_label, description_label):
            widget.bind("<Enter>", _on_enter)
            widget.bind("<Leave>", _on_leave)
            widget.bind("<Button-1>", _on_click)

        self.tile_buttons.append(
            {
                "outer": outer,
                "shadow": shadow,
                "tile": tile,
                "color": color,
                "title": title_label,
                "description": description_label,
            }
        )
        return outer

    def _lighten_color(self, color: str, factor: float) -> str:
        color = color.lstrip("#")
        if len(color) != 6:
            return color
        r = int(color[0:2], 16)
        g = int(color[2:4], 16)
        b = int(color[4:6], 16)
        r = min(255, int(r + (255 - r) * factor))
        g = min(255, int(g + (255 - g) * factor))
        b = min(255, int(b + (255 - b) * factor))
        return f"#{r:02x}{g:02x}{b:02x}"

    def _clear_search(self) -> None:
        self.search_var.set("")

    def _on_search_change(self, *_args: object) -> None:
        self.load_items()

    def _on_auto_save_toggle(self) -> None:
        if self._auto_save_job:
            self.root.after_cancel(self._auto_save_job)
            self._auto_save_job = None
        if self.auto_save_var.get():
            self.log_activity("Auto-save enabled")
            self._auto_save_job = self.root.after(60_000, self._run_auto_save)
        else:
            self.log_activity("Auto-save paused")

    def _run_auto_save(self) -> None:
        self.log_activity("Auto-saved inventory snapshot")
        if self.auto_save_var.get():
            self._auto_save_job = self.root.after(60_000, self._run_auto_save)

    def _format_activity_log(self) -> str:
        recent = self.activity_log_entries[:3]
        return "Last 3 actions: " + ", ".join(recent)

    def log_activity(self, action: str) -> None:
        timestamp = datetime.now().strftime("%H:%M")
        entry = f"{action} ({timestamp})"
        self.activity_log_entries.insert(0, entry)
        del self.activity_log_entries[3:]
        self.activity_log_var.set(self._format_activity_log())

    def _update_user_status(self) -> None:
        self.user_status_var.set(
            f"Signed in as {self.current_user} | {self.sync_status_badge_var.get()}"
        )
        self.last_sync_var.set(self._sync_detail_default)
        if self._last_loaded_items:
            self.update_totals(list(self._last_loaded_items))

    def _build_dashboard_view(self, container: ttk.Frame) -> None:
        container.columnconfigure(0, weight=1)

        header = ttk.Frame(container)
        header.pack(fill=tk.X, pady=(0, 12))
        status_panel = ttk.Frame(header)
        status_panel.pack(side=tk.RIGHT, anchor=tk.E)
        status_panel.columnconfigure(0, weight=1)
        self.sync_badge = tk.Label(
            status_panel,
            textvariable=self.sync_status_badge_var,
            font=("Segoe UI", 10, "bold"),
            padx=10,
            pady=4,
        )
        self.sync_badge.grid(row=0, column=0, sticky="e", pady=(0, 4))
        self.sync_detail_label = ttk.Label(status_panel, textvariable=self.sync_detail_var, style="Hint.TLabel")
        self.sync_detail_label.grid(row=1, column=0, sticky="e")
        self.sync_pending_label = ttk.Label(status_panel, textvariable=self.sync_pending_var, style="Hint.TLabel")
        self.sync_pending_label.grid(row=2, column=0, sticky="e", pady=(2, 0))
        button_panel = ttk.Frame(status_panel)
        button_panel.grid(row=0, column=1, rowspan=3, padx=(12, 0), sticky="ns")
        self.sync_now_button = ttk.Button(button_panel, text="Sync Now", command=self.on_sync_now)
        self.sync_now_button.pack(side=tk.TOP, fill=tk.X)
        self.open_sheet_button = ttk.Button(button_panel, text="Open Sheet", command=self.on_open_sheet)
        self.open_sheet_button.pack(side=tk.TOP, fill=tk.X, pady=(6, 0))
        self._update_sync_badge(not self._offline)
        self.user_label = ttk.Label(header, textvariable=self.user_status_var, style="SubHeader.TLabel")
        self.user_label.pack(side=tk.LEFT)

        title = ttk.Label(container, text="RugBase Inventory System", style="Hero.TLabel")
        title.pack(anchor="center", pady=(0, 12))

        tile_frame = ttk.Frame(container)
        tile_frame.pack(fill=tk.X, pady=(0, 16))
        tile_specs = [
            (
                "Inventory",
                "#0078D7",
                "Review live stock levels and edit catalog details.",
                lambda: self.show_view("dashboard"),
            ),
            (
                "Customers",
                "#D13438",
                "Manage client profiles, notes, and outreach.",
                lambda: self.show_view("customers"),
            ),
            (
                "Sales / Consignments",
                "#107C10",
                "Open consignments, returns, and point-of-sale tools.",
                self.open_consignment_list,
            ),
            (
                "Reports",
                "#F7630C",
                "Analyze performance trends and export snapshots.",
                lambda: self.show_view("reports"),
            ),
            (
                "Sync & Backup",
                "#6B69D6",
                "Launch Google Sheets sync and local backup workflows.",
                lambda: self.show_view("sync"),
            ),
            (
                "Label Generator",
                "#0B5ED7",
                "Print DYMO labels directly from the inventory.",
                self.open_label_generator,
            ),
        ]

        for index, spec in enumerate(tile_specs):
            tile = self._create_tile(tile_frame, *spec)
            row, column = divmod(index, 3)
            tile.grid(row=row, column=column, padx=12, pady=12, sticky="nsew")
            tile_frame.grid_columnconfigure(column, weight=1)

        main_card = ttk.Frame(container, style="Card.TFrame")
        main_card.pack(fill=tk.BOTH, expand=True)

        search_row = ttk.Frame(main_card)
        search_row.pack(fill=tk.X, pady=(0, 12))

        ttk.Label(search_row, text="Live Search").pack(side=tk.LEFT)
        self.search_entry = ttk.Entry(search_row, textvariable=self.search_var)
        self.search_entry.pack(side=tk.LEFT, padx=(12, 8), fill=tk.X, expand=True)
        self.clear_search_button = ttk.Button(
            search_row, text="Clear", command=self._clear_search
        )
        self.clear_search_button.pack(side=tk.LEFT, padx=(0, 8))

        self.sold_filter_check = ttk.Checkbutton(
            search_row,
            text="Sold items",
            variable=self.show_sold_var,
            command=self.load_items,
        )
        self.sold_filter_check.pack(side=tk.LEFT)

        tools_frame = ttk.Frame(search_row)
        tools_frame.pack(side=tk.RIGHT)

        self.column_status_label = ttk.Label(
            tools_frame,
            textvariable=self.column_status_var,
            style="Info.TLabel",
        )
        self.column_status_label.pack(side=tk.RIGHT)
        self.column_check_button = ttk.Button(
            tools_frame,
            text="Auto Repair Columns",
            command=self.on_check_columns,
        )
        self.column_check_button.pack(side=tk.RIGHT, padx=(0, 8))

        hint = ttk.Label(
            main_card,
            text="Filter by Rug No, Collection, Style, or Color. Matches update instantly as you type.",
            style="Hint.TLabel",
            wraplength=720,
        )
        hint.pack(fill=tk.X, pady=(0, 8))

        self.table_frame = ttk.Frame(main_card)
        self.table_frame.pack(fill=tk.BOTH, expand=True)
        self.table_frame.columnconfigure(0, weight=1)
        self.table_frame.rowconfigure(0, weight=1)

        self.column_defs = list(db.MASTER_SHEET_COLUMNS)
        self.columns = [field for field, _ in self.column_defs]

        self.tree = ttk.Treeview(self.table_frame, columns=self.columns, show="headings")
        for field, header_text in self.column_defs:
            anchor = tk.E if field in db.NUMERIC_FIELDS else tk.W
            self.tree.heading(field, text=header_text)
            self.tree.column(field, anchor=anchor, width=150, stretch=False)

        self.tree.tag_configure("oddrow", background="#f5f9ff")
        self.tree.tag_configure("evenrow", background="#ffffff")

        yscroll = ttk.Scrollbar(self.table_frame, orient=tk.VERTICAL, command=self.tree.yview)
        xscroll = ttk.Scrollbar(self.table_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        self.tree_menu = tk.Menu(self.tree, tearoff=0)
        self.tree_menu.add_command(label="Mark as Sold...", command=self.open_mark_sold_modal)
        self.tree.bind("<Button-3>", self.on_tree_right_click)
        self.tree.bind("<Double-1>", self.on_tree_double_click)

        self.grand_total_var = tk.StringVar(value="Grand Total: $0.00")
        totals_frame = ttk.Frame(main_card)
        totals_frame.pack(fill=tk.X, pady=(12, 0))
        ttk.Label(totals_frame, textvariable=self.grand_total_var, style="SubHeader.TLabel").pack(
            side=tk.LEFT
        )

        actions_frame = ttk.Frame(main_card)
        actions_frame.pack(fill=tk.X, pady=(16, 0))

        self.add_button = ttk.Button(
            actions_frame, text="Add Item", style="Accent.TButton", command=self.on_add_item
        )
        self.add_button.pack(side=tk.LEFT)
        self._offline_controls.append(self.add_button)

        self.open_button = ttk.Button(
            actions_frame, text="Open Selected", command=self.on_open_item
        )
        self.open_button.pack(side=tk.LEFT, padx=(8, 0))
        self._offline_controls.append(self.open_button)

        self.label_button = ttk.Button(
            actions_frame, text="Generate Label (DYMO)", command=self.open_label_generator
        )
        self.label_button.pack(side=tk.LEFT, padx=(8, 0))
        self._offline_controls.append(self.label_button)

        self.consignment_button = ttk.Button(
            actions_frame, text="Consignment Out", command=self.open_consignment_modal
        )
        self.consignment_button.pack(side=tk.LEFT, padx=(8, 0))
        self._offline_controls.append(self.consignment_button)

        self.return_button = ttk.Button(
            actions_frame, text="Consignment Returns", command=self.open_return_modal
        )
        self.return_button.pack(side=tk.LEFT, padx=(8, 0))
        self._offline_controls.append(self.return_button)

        self.consignment_list_button = ttk.Button(
            actions_frame, text="View Consignments", command=self.open_consignment_list
        )
        self.consignment_list_button.pack(side=tk.LEFT, padx=(8, 0))

        self.delete_button = ttk.Button(
            actions_frame, text="Delete Item", command=self.on_delete_item
        )
        self.delete_button.pack(side=tk.LEFT, padx=(8, 0))
        self._offline_controls.append(self.delete_button)

        export_frame = ttk.Frame(main_card)
        export_frame.pack(fill=tk.X, pady=(12, 0))
        self.import_csv_button = ttk.Button(export_frame, text="Import CSV", command=self.on_import_csv)
        self.import_csv_button.pack(side=tk.LEFT)
        self._offline_controls.append(self.import_csv_button)
        self.import_xml_button = ttk.Button(export_frame, text="Import XML", command=self.on_import_xml)
        self.import_xml_button.pack(side=tk.LEFT, padx=(8, 0))
        self._offline_controls.append(self.import_xml_button)
        self.export_csv_button = ttk.Button(export_frame, text="Export CSV", command=self.on_export_csv)
        self.export_csv_button.pack(side=tk.LEFT, padx=(8, 0))
        self._offline_controls.append(self.export_csv_button)
        ttk.Button(export_frame, text="Export XLSX", command=self.on_export_xlsx).pack(
            side=tk.LEFT, padx=(8, 0)
        )

        automation_bar = ttk.Frame(main_card)
        automation_bar.pack(fill=tk.X, pady=(16, 0))
        ttk.Checkbutton(
            automation_bar,
            text="Auto-save to Excel every 60s",
            variable=self.auto_save_var,
            command=self._on_auto_save_toggle,
        ).pack(side=tk.LEFT)

        ttk.Label(
            automation_bar,
            textvariable=self.activity_log_var,
            style="SubHeader.TLabel",
        ).pack(side=tk.RIGHT)

        self.sales_report_frame = ttk.LabelFrame(
            container, text="Sales Report", padding=16
        )
        self.sales_report_frame.pack(fill=tk.X, pady=(16, 0))
        range_frame = ttk.Frame(self.sales_report_frame)
        range_frame.pack(fill=tk.X)
        ttk.Label(range_frame, text="Range:").pack(side=tk.LEFT)
        ttk.Button(
            range_frame, text="30d", command=lambda: self.set_sales_report_period(30)
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            range_frame, text="90d", command=lambda: self.set_sales_report_period(90)
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            range_frame, text="All", command=lambda: self.set_sales_report_period(None)
        ).pack(side=tk.LEFT, padx=(6, 0))

        self.sales_report_var = tk.StringVar(value="Sales data not available.")
        ttk.Label(
            self.sales_report_frame,
            textvariable=self.sales_report_var,
            style="SubHeader.TLabel",
        ).pack(fill=tk.X, pady=(6, 0))

        self.footer_frame = ttk.Frame(container, padding=(0, 12, 0, 0))
        self.footer_frame.pack(fill=tk.X, pady=(16, 0))

        self.summary_label = ttk.Label(self.footer_frame, textvariable=self.summary_var)
        self.summary_label.pack(side=tk.LEFT)

        self.update_button = ttk.Button(
            self.footer_frame, text="Check for Updates", command=self.on_check_for_updates
        )
        self.update_button.pack(side=tk.RIGHT)

        self.version_label = ttk.Label(
            self.footer_frame,
            text=f"Version {__version__}",
            style="SubHeader.TLabel",
        )
        self.version_label.pack(side=tk.RIGHT, padx=(0, 12))

    def load_items(self) -> None:
        for row in self.tree.get_children():
            self.tree.delete(row)

        items = self.get_filtered_rows()
        self._last_loaded_items = list(items)
        for index, item in enumerate(items):
            tag = "evenrow" if index % 2 == 0 else "oddrow"
            self.tree.insert(
                "",
                tk.END,
                iid=item["item_id"],
                values=self._format_item_values(item),
                tags=(tag,),
            )

        self.update_totals(items)
        self.refresh_sales_report()
        self._autosize_columns()

    def on_tree_double_click(self, event: tk.Event) -> None:
        self.open_selected_item()

    def on_tree_right_click(self, event: tk.Event) -> None:
        row_id = self.tree.identify_row(event.y)
        if row_id:
            self.tree.selection_set(row_id)
        if not hasattr(self, "tree_menu"):
            return
        try:
            self.tree_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.tree_menu.grab_release()

    def on_open_item(self) -> None:
        self.open_selected_item()

    def on_add_item(self) -> None:
        self.log_activity("Opened new inventory item")
        ItemCardWindow(self.root, None, on_save=self.load_items)

    def open_label_generator(self) -> None:
        if self.label_window and self.label_window.window.winfo_exists():
            self.label_window.window.focus_set()
            return
        self.label_window = LabelGeneratorWindow(self.root, on_close=self._clear_label_window)
        self.label_window.apply_theme(self._palette)
        self.log_activity("Opened label generator")

    def on_open_label_generator(self, _event: tk.Event) -> None:
        self.open_label_generator()

    def _clear_label_window(self) -> None:
        self.label_window = None

    def open_consignment_modal(self) -> None:
        messagebox.showinfo(
            "Consignment",
            "Consignment workflows have not been migrated to Google Sheets synchronization.",
        )

    def open_return_modal(self) -> None:
        messagebox.showinfo(
            "Consignment",
            "Consignment workflows have not been migrated to Google Sheets synchronization.",
        )

    def on_check_columns(self) -> None:
        try:
            added_columns = db.ensure_inventory_columns()
        except Exception as exc:  # pragma: no cover - defensive UI handler
            logger.exception("Column check failed")
            messagebox.showerror(
                "Column Check Failed",
                f"Column check failed: {exc}",
            )
            self.column_status_var.set("Column check failed.")
            return

        if added_columns:
            message = self._format_column_check_message(added_columns)
            messagebox.showinfo("Inventory Columns", message)
        else:
            message = "All required columns are present."
            messagebox.showinfo("Inventory Columns", message)
        self.column_status_var.set(message)
        self.log_activity("Column audit complete")

    def open_consignment_list(self) -> None:
        messagebox.showinfo(
            "Consignment",
            "Consignment workflows have not been migrated to Google Sheets synchronization.",
        )

    def get_selected_item_id(self) -> Optional[str]:
        selected = self.tree.selection()
        if not selected:
            return None
        return selected[0]

    def open_selected_item(self) -> None:
        item_id = self.get_selected_item_id()
        if not item_id:
            messagebox.showinfo("Open Item", "Please select an item to open.")
            return

        ItemCardWindow(self.root, item_id, on_save=self.load_items)

    def open_mark_sold_modal(self) -> None:
        item_id = self.get_selected_item_id()
        if not item_id:
            messagebox.showinfo("Mark as Sold", "Please select an item to mark as sold.")
            return

        item = db.fetch_item(item_id)
        if not item:
            messagebox.showerror("Mark as Sold", "Unable to load the selected item.")
            return

        MarkSoldDialog(
            self.root,
            item,
            on_complete=self._on_sale_completed,
            on_customer_created=self._on_customer_created_from_sale,
        )

    def _on_sale_completed(self) -> None:
        self.load_items()
        self.log_activity("Sale completed")

    def _on_customer_created_from_sale(self, _record: Dict[str, Any]) -> None:
        self.load_customers()
        self.log_activity("Customer added from sale")

    def _build_customers_view(self, container: ttk.Frame) -> None:
        container.columnconfigure(0, weight=3)
        container.columnconfigure(1, weight=2)
        container.rowconfigure(1, weight=1)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 12))
        ttk.Label(header, text="Customers", style="Header.TLabel").pack(anchor=tk.W)
        ttk.Label(
            header,
            text="Maintain customer contact details, notes, and purchase history.",
            style="Hint.TLabel",
        ).pack(anchor=tk.W, pady=(2, 0))

        list_frame = ttk.LabelFrame(container, text="Customer List", padding=12)
        list_frame.grid(row=1, column=0, sticky="nsew", padx=(0, 12))
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(1, weight=1)

        search_frame = ttk.Frame(list_frame)
        search_frame.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        search_frame.columnconfigure(1, weight=1)

        ttk.Label(search_frame, text="Search:").grid(row=0, column=0, padx=(0, 6))
        self.customer_search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=self.customer_search_var)
        search_entry.grid(row=0, column=1, sticky="ew")
        ttk.Button(search_frame, text="Search", command=self.on_customer_search).grid(
            row=0, column=2, padx=(6, 0)
        )
        ttk.Button(search_frame, text="Clear", command=self.on_customer_clear).grid(
            row=0, column=3, padx=(6, 0)
        )

        columns = ("full_name", "phone", "email", "city", "updated_at")
        self.customer_tree = ttk.Treeview(list_frame, columns=columns, show="headings", height=16)
        headings = {
            "full_name": "Full Name",
            "phone": "Phone",
            "email": "Email",
            "city": "City",
            "updated_at": "Updated",
        }
        for column in columns:
            anchor = tk.W
            width = 180 if column == "full_name" else 140
            self.customer_tree.heading(column, text=headings[column])
            self.customer_tree.column(column, anchor=anchor, width=width, stretch=True)

        self.customer_tree.grid(row=1, column=0, sticky="nsew")
        yscroll = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.customer_tree.yview)
        self.customer_tree.configure(yscrollcommand=yscroll.set)
        yscroll.grid(row=1, column=1, sticky="ns")

        button_row = ttk.Frame(list_frame)
        button_row.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        ttk.Button(button_row, text="Edit Selected", command=self.on_edit_customer).pack(side=tk.LEFT)
        self.customer_tree.bind("<Double-1>", self.on_customer_double_click)

        form_frame = ttk.LabelFrame(container, text="Add Customer", padding=12)
        form_frame.grid(row=1, column=1, sticky="nsew")
        form_frame.columnconfigure(1, weight=1)

        form_fields = [
            ("Full Name", "full_name"),
            ("Phone", "phone"),
            ("Email", "email"),
            ("Address", "address"),
            ("City", "city"),
            ("State", "state"),
            ("Zip", "zip"),
        ]

        self.customer_form_vars.clear()

        for index, (label, key) in enumerate(form_fields):
            ttk.Label(form_frame, text=f"{label}:").grid(
                row=index, column=0, sticky=tk.W, pady=4, padx=(0, 8)
            )
            variable = tk.StringVar()
            self.customer_form_vars[key] = variable
            entry = ttk.Entry(form_frame, textvariable=variable)
            entry.grid(row=index, column=1, sticky="ew", pady=4)

        notes_row = len(form_fields)
        ttk.Label(form_frame, text="Notes:").grid(
            row=notes_row, column=0, sticky=tk.NW, pady=4, padx=(0, 8)
        )
        self.customer_notes_text = tk.Text(form_frame, height=5, width=32)
        self.customer_notes_text.grid(row=notes_row, column=1, sticky="ew", pady=4)

        action_row = ttk.Frame(form_frame)
        action_row.grid(row=notes_row + 1, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(action_row, text="Clear", command=self.clear_customer_form).pack(side=tk.RIGHT)
        ttk.Button(
            action_row,
            text="Add Customer",
            style="Accent.TButton",
            command=self.save_customer_from_form,
        ).pack(side=tk.RIGHT, padx=(0, 8))

        self.customer_tree.bind("<<TreeviewSelect>>", self.on_customer_select)
        self.customer_search_var.trace_add("write", lambda *_: self.filter_customer_records())

    def _build_sync_view(self, container: ttk.Frame) -> None:
        ttk.Label(container, text="Sync & Backup", style="Header.TLabel").pack(anchor=tk.W)
        ttk.Label(
            container,
            text="Synchronize with Google Sheets, review history, and manage backups.",
            style="Hint.TLabel",
        ).pack(anchor=tk.W, pady=(2, 12))
        ttk.Button(
            container,
            text="Open Google Sheets",
            command=self.on_open_sheet,
        ).pack(anchor=tk.W, pady=(0, 12))
        ttk.Button(
            container,
            text="Upload credentials.json",
            command=self._on_upload_credentials,
        ).pack(anchor=tk.W)

    def _build_placeholder_view(
        self, container: ttk.Frame, title: str, description: str
    ) -> None:
        container.columnconfigure(0, weight=1)
        ttk.Label(container, text=title, style="Header.TLabel").grid(
            row=0, column=0, sticky="w", pady=(0, 6)
        )
        ttk.Label(container, text=description, style="Hint.TLabel", wraplength=640).grid(
            row=1, column=0, sticky="w"
        )

    def load_customers(self) -> None:
        if not self.customer_tree:
            return
        search_term = getattr(self, "customer_search_var", None)
        query = search_term.get().strip() if search_term else ""
        try:
            customers = db.fetch_customers(query or None)
        except Exception as exc:
            messagebox.showerror("Customers", f"Failed to load customers: {exc}")
            return

        for item in self.customer_tree.get_children():
            self.customer_tree.delete(item)

        self.customer_records = {}
        for record in customers:
            raw_id = record.get("id")
            if raw_id is None:
                continue
            customer_id = str(raw_id)
            self.customer_records[customer_id] = record
            updated = record.get("updated_at") or ""
            if updated:
                updated = str(updated).replace("T", " ")
            values = (
                record.get("full_name") or "",
                record.get("phone") or "",
                record.get("email") or "",
                record.get("city") or "",
                updated,
            )
            self.customer_tree.insert("", tk.END, iid=customer_id, values=values)

    def filter_customer_records(self) -> None:
        """Refresh the customer list based on the current search entry."""

        # ``load_customers`` already applies the search term, so simply reuse it.
        # Keeping the filtering logic in a single place avoids subtle
        # discrepancies between incremental filtering and explicit refreshes.
        self.load_customers()

    def on_customer_select(self, _event: tk.Event) -> None:
        """Cache the currently selected customer for later interactions."""

        customer_id = self.get_selected_customer_id()
        if customer_id is None:
            return

        if customer_id not in self.customer_records:
            try:
                record = db.fetch_customer(customer_id)
            except Exception:
                logger.exception("Failed to fetch selected customer", extra={"id": customer_id})
                return
            if record:
                self.customer_records[customer_id] = record

    def on_customer_search(self) -> None:
        self.load_customers()

    def on_customer_clear(self) -> None:
        if hasattr(self, "customer_search_var"):
            self.customer_search_var.set("")
        self.load_customers()

    def get_selected_customer_id(self) -> Optional[str]:
        if not self.customer_tree:
            return None
        selected = self.customer_tree.selection()
        if not selected:
            return None
        return selected[0]

    def on_customer_double_click(self, _event: tk.Event) -> None:
        customer_id = self.get_selected_customer_id()
        if customer_id is not None:
            self.open_customer_dialog(customer_id)

    def on_edit_customer(self) -> None:
        customer_id = self.get_selected_customer_id()
        if customer_id is None:
            messagebox.showinfo("Customers", "Please select a customer to edit.")
            return
        self.open_customer_dialog(customer_id)

    def open_customer_dialog(self, customer_id: Optional[str] = None) -> None:
        record = None
        if customer_id is not None:
            record = db.fetch_customer(customer_id)
            if not record:
                messagebox.showerror("Customers", "Customer could not be loaded.")
                return
        CustomerDialog(self.root, customer=record, on_save=self._on_customer_saved)

    def _on_customer_saved(self, record: Dict[str, Any]) -> None:
        self.load_customers()
        if record.get("id"):
            self.customer_records[str(record["id"])] = record
        self.log_activity("Customer saved")

    def save_customer_from_form(self) -> None:
        data = {key: var.get().strip() for key, var in self.customer_form_vars.items()}
        notes = ""
        if self.customer_notes_text:
            notes = self.customer_notes_text.get("1.0", tk.END).strip()
        data["notes"] = notes
        try:
            customer_id = db.create_customer(data)
        except ValueError as exc:
            messagebox.showerror("Add Customer", str(exc))
            return
        except Exception as exc:
            messagebox.showerror("Add Customer", f"Failed to add customer: {exc}")
            return

        self.clear_customer_form()
        self.load_customers()
        record = db.fetch_customer(customer_id)
        if record:
            messagebox.showinfo("Add Customer", "Customer added successfully.")
            self.customer_records[str(customer_id)] = record
            self.log_activity("Added customer")

    def clear_customer_form(self) -> None:
        for variable in self.customer_form_vars.values():
            variable.set("")
        if self.customer_notes_text:
            self.customer_notes_text.delete("1.0", tk.END)

    def on_delete_item(self) -> None:
        item_id = self.get_selected_item_id()
        if not item_id:
            messagebox.showinfo("Delete Item", "Please select an item to delete.")
            return

        item = db.fetch_item(item_id)
        item_label = item.get("rug_no") if item else item_id
        confirm = messagebox.askyesno(
            "Delete Item",
            f"Are you sure you want to delete {item_label}? This action cannot be undone.",
        )
        if not confirm:
            return

        try:
            db.delete_item(item_id)
        except Exception as exc:  # sqlite3.Error, but keep generic to avoid extra import
            messagebox.showerror("Delete Item", f"Failed to delete the item: {exc}")
            return

        self.load_items()
        messagebox.showinfo("Delete Item", "The selected item has been deleted.")
        self.log_activity(f"Deleted item {item_label}")

    def get_filtered_rows(self) -> list[dict]:
        status_filter = "sold" if self.show_sold_var.get() else None
        try:
            items = db.fetch_items(status_filter=status_filter)
        except Exception as exc:
            messagebox.showerror("Inventory", f"Failed to load inventory: {exc}")
            return []

        query = self.search_var.get().strip().lower()
        if not query:
            return items

        tokens = query.split()
        filtered: list[dict] = []
        for item in items:
            haystacks = [
                str(item.get("rug_no", "")).lower(),
                str(item.get("collection", "")).lower(),
                str(item.get("style", "")).lower(),
                str(item.get("ground", "")).lower(),
                str(item.get("border", "")).lower(),
            ]
            if all(any(token in hay for hay in haystacks) for token in tokens):
                filtered.append(item)
        return filtered

    def _format_item_values(self, item: dict) -> tuple[str, ...]:
        values: list[str] = []
        for field, _ in self.column_defs:
            value = item.get(field)
            if field in db.NUMERIC_FIELDS:
                if value is None or value == "":
                    values.append("")
                else:
                    try:
                        values.append(f"{float(value):.2f}")
                    except (TypeError, ValueError):
                        values.append(str(value))
            else:
                values.append(str(value) if value is not None else "")
        return tuple(values)

    def _autosize_columns(self) -> None:
        self.tree.update_idletasks()
        default_font = tkfont.nametofont("TkDefaultFont")
        padding = 20
        max_width = 280

        for field, header in self.column_defs:
            header_width = default_font.measure(header) + padding
            content_width = header_width
            for item_id in self.tree.get_children():
                text = self.tree.set(item_id, field)
                content_width = max(content_width, default_font.measure(text) + padding)
            final_width = max(80, min(content_width, max_width))
            self.tree.column(field, width=final_width, stretch=False)

    def update_totals(self, items: list[dict]) -> None:
        total_items = len(items)
        total_area = 0.0
        for item in items:
            value = item.get("area")
            try:
                total_area += float(value or 0)
            except (TypeError, ValueError):
                continue
        summary = (
            f"Total Items: {total_items} | Total Area: {total_area:.2f} sq ft | Last Sync: {self.last_sync_var.get()}"
        )
        self.summary_var.set(summary)

        total_value = 0.0
        for item in items:
            value = item.get("sale_price")
            if value in (None, ""):
                value = item.get("sp")
            try:
                total_value += float(value or 0)
            except (TypeError, ValueError):
                continue
        self.grand_total_var.set(f"Grand Total: ${total_value:,.2f}")

    def refresh_sales_report(self) -> None:
        if not hasattr(self, "sales_report_var"):
            return
        self.set_sales_report_period(self.sales_period_days)

    def set_sales_report_period(self, days: Optional[int]) -> None:
        self.sales_period_days = days
        try:
            summary = db.get_sales_summary(days)
        except Exception as exc:
            self.sales_report_var.set(f"Sales report unavailable: {exc}")
            return

        if days is None:
            label = "All time"
        else:
            label = f"Last {days} days"

        count = summary.get("count", 0) or 0
        total = summary.get("total", 0.0) or 0.0
        average = summary.get("average", 0.0) or 0.0
        self.sales_report_var.set(
            f"{label}: {count} sale(s) • Revenue ${total:,.2f} • Average ${average:,.2f}"
        )

    def on_export_csv(self) -> None:
        items = self.get_filtered_rows()
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=(("CSV files", "*.csv"), ("All files", "*.*")),
            title="Export CSV",
        )
        if not file_path:
            return

        try:
            with open(file_path, "w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow([header for _, header in self.column_defs])
                for item in items:
                    writer.writerow(self._format_item_values(item))
        except OSError as exc:
            messagebox.showerror("Export CSV", f"Failed to export CSV: {exc}")
        else:
            self.log_activity("Exported CSV")

    def on_export_xlsx(self) -> None:
        items = self.get_filtered_rows()
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=(("Excel files", "*.xlsx"), ("All files", "*.*")),
            title="Export XLSX",
        )
        if not file_path:
            return

        workbook = Workbook()
        sheet = workbook.active
        sheet.append([header for _, header in self.column_defs])
        for item in items:
            sheet.append(self._format_item_values(item))

        try:
            workbook.save(file_path)
        except OSError as exc:
            messagebox.showerror("Export XLSX", f"Failed to export XLSX: {exc}")
        else:
            self.log_activity("Exported XLSX")

    def on_check_for_updates(self) -> None:
        """Prompt the user to download the latest RugBase release."""

        threading.Thread(
            target=lambda: updater.check_for_updates(self.root),
            name="UpdateCheckWorker",
            daemon=True,
        ).start()

    def _handle_import_result(self, result: importer.ImportResult, title: str) -> None:
        self.load_items()
        messagebox.showinfo(
            title,
            (
                f"Processed {result.total} rows.\n"
                f"Inserted: {result.inserted}\n"
                f"Updated: {result.updated}\n"
                f"Skipped: {result.skipped}"
            ),
        )

    def on_import_csv(self) -> None:
        file_path = filedialog.askopenfilename(
            filetypes=(("CSV files", "*.csv"), ("All files", "*.*")),
            title="Import CSV",
        )
        if not file_path:
            return

        try:
            result = importer.import_csv(file_path)
        except importer.ImporterError as exc:
            messagebox.showerror("Import CSV", str(exc))
            return

        self._handle_import_result(result, "Import CSV")
        self.log_activity("Imported CSV")

    def on_import_xml(self) -> None:
        file_path = filedialog.askopenfilename(
            filetypes=(("XML files", "*.xml"), ("All files", "*.*")),
            title="Import XML",
        )
        if not file_path:
            return

        try:
            result = importer.import_xml(file_path)
        except importer.ImporterError as exc:
            messagebox.showerror("Import XML", str(exc))
            return

        self._handle_import_result(result, "Import XML")
        self.log_activity("Imported XML")

    def _build_menu(self) -> None:
        menubar = tk.Menu(self.root)
        tools_menu = tk.Menu(menubar, tearoff=0)
        tools_menu.add_command(label="Open Debug Log", command=self.open_debug_log)
        tools_menu.add_command(label="Open Data Folder", command=self.open_data_folder)
        tools_menu.add_command(
            label="Sync Settings", command=self._open_sync_settings_window
        )
        tools_menu.add_separator()
        tools_menu.add_checkbutton(
            label="Enable Night Mode",
            variable=self.dark_mode_var,
            command=self._toggle_dark_mode,
        )
        menubar.add_cascade(label="Tools", menu=tools_menu)
        self.root.config(menu=menubar)

    def open_debug_log(self) -> None:
        log_path = get_log_path()
        if not log_path.exists():
            messagebox.showinfo("Debug Log", "No debug log is available yet.", parent=self.root)
            return

        try:
            content = log_path.read_text(encoding="utf-8")
        except OSError as exc:
            messagebox.showerror("Debug Log", f"Unable to read log file: {exc}", parent=self.root)
            return

        palette = self._palette or self._light_palette()
        window = tk.Toplevel(self.root)
        window.title("RugBase Debug Log")
        window.geometry("720x480")
        window.transient(self.root)
        window.configure(bg=palette["background"])

        text_widget = tk.Text(window, wrap="none")
        text_widget.insert("1.0", content or "(Log file is empty)")
        try:
            fixed_font = tkfont.nametofont("TkFixedFont")
        except tk.TclError:
            fixed_font = ("Consolas", 10)
        text_widget.configure(
            state="disabled",
            font=fixed_font,
            background=palette["frame"],
            foreground=palette["foreground"],
            highlightbackground=palette["border"],
            highlightcolor=palette["border"],
            selectbackground=palette["selection"],
            selectforeground=palette["selection_text"],
            insertbackground=palette["foreground"],
        )

        yscroll = ttk.Scrollbar(window, orient="vertical", command=text_widget.yview)
        xscroll = ttk.Scrollbar(window, orient="horizontal", command=text_widget.xview)
        text_widget.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        text_widget.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        window.rowconfigure(0, weight=1)
        window.columnconfigure(0, weight=1)

        ttk.Label(window, text=str(log_path), style="Hint.TLabel").grid(
            row=2, column=0, columnspan=2, sticky="w", padx=8, pady=(6, 10)
        )

    def open_data_folder(self) -> None:
        path = app_paths.APP_DIR
        path.mkdir(parents=True, exist_ok=True)
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path)])
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except Exception as exc:
            messagebox.showerror(
                "Data Folder",
                f"Folder could not be opened: {exc}\n{path}",
                parent=self.root,
            )

    def _on_close(self) -> None:
        if hasattr(self, "sync_manager"):
            try:
                self.sync_manager.shutdown()
            except Exception:  # pragma: no cover - defensive cleanup
                pass
        self.root.destroy()


class CustomerDialog:
    def __init__(
        self,
        parent: tk.Misc,
        customer: Optional[Dict[str, Any]] = None,
        *,
        on_save: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.parent = parent
        self.customer = customer or {}
        self.on_save = on_save
        self.window = tk.Toplevel(parent)
        self.window.title("Edit Customer" if customer else "New Customer")
        self.window.transient(parent)
        self.window.grab_set()
        self.window.resizable(False, False)

        self.form_vars: Dict[str, tk.StringVar] = {}

        frame = ttk.Frame(self.window, padding=12)
        frame.grid(row=0, column=0, sticky="nsew")
        frame.columnconfigure(1, weight=1)

        fields = [
            ("Full Name", "full_name"),
            ("Phone", "phone"),
            ("Email", "email"),
            ("Address", "address"),
            ("City", "city"),
            ("State", "state"),
            ("Zip", "zip"),
        ]

        for index, (label, key) in enumerate(fields):
            ttk.Label(frame, text=f"{label}:").grid(row=index, column=0, sticky=tk.W, pady=4, padx=(0, 8))
            variable = tk.StringVar(value=str(self.customer.get(key) or ""))
            self.form_vars[key] = variable
            entry = ttk.Entry(frame, textvariable=variable, width=32)
            entry.grid(row=index, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="Notes:").grid(
            row=len(fields), column=0, sticky=tk.NW, pady=4, padx=(0, 8)
        )
        self.notes_text = tk.Text(frame, height=4, width=40)
        self.notes_text.grid(row=len(fields), column=1, sticky="ew", pady=4)
        self.notes_text.insert("1.0", str(self.customer.get("notes") or ""))

        button_row = ttk.Frame(frame)
        button_row.grid(row=len(fields) + 1, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(button_row, text="Cancel", command=self.window.destroy).pack(side=tk.RIGHT)
        ttk.Button(
            button_row,
            text="Save",
            style="Accent.TButton",
            command=self._save,
        ).pack(side=tk.RIGHT, padx=(0, 8))

    def _save(self) -> None:
        data = {key: var.get().strip() for key, var in self.form_vars.items()}
        data["notes"] = self.notes_text.get("1.0", tk.END).strip()
        try:
            if self.customer.get("id"):
                customer_id = str(self.customer["id"])
                db.update_customer(customer_id, data)
            else:
                customer_id = db.create_customer(data)
        except ValueError as exc:
            messagebox.showerror("Customer", str(exc))
            return
        except Exception as exc:
            messagebox.showerror("Customer", f"Failed to save customer: {exc}")
            return

        record = db.fetch_customer(customer_id)
        if record and self.on_save:
            self.on_save(record)

        messagebox.showinfo("Customer", "Customer saved successfully.")
        self.window.destroy()


class MarkSoldDialog:
    def __init__(
        self,
        parent: tk.Misc,
        item: Dict[str, Any],
        *,
        on_complete: Optional[Callable[[], None]] = None,
        on_customer_created: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.parent = parent
        self.item = item
        self.on_complete = on_complete
        self.on_customer_created = on_customer_created
        self.window = tk.Toplevel(parent)
        self.window.title("Mark as Sold")
        self.window.transient(parent)
        self.window.grab_set()
        self.window.resizable(False, False)

        container = ttk.Frame(self.window, padding=12)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(1, weight=1)

        summary = item.get("rug_no") or item.get("item_id")
        ttk.Label(
            container,
            text=f"Item: {summary}",
            style="SubHeader.TLabel",
        ).grid(row=0, column=0, columnspan=2, sticky=tk.W, pady=(0, 12))

        ttk.Label(container, text="Customer:").grid(row=1, column=0, sticky=tk.W, pady=4, padx=(0, 8))
        self.customer_combo = ttk.Combobox(container, state="readonly")
        self.customer_combo.grid(row=1, column=1, sticky="ew", pady=4)

        self.customer_options: List[Dict[str, Any]] = []

        ttk.Button(
            container,
            text="New Customer",
            command=self._open_new_customer,
        ).grid(row=2, column=1, sticky=tk.E, pady=(0, 8))

        ttk.Label(container, text="Sale Price:").grid(row=3, column=0, sticky=tk.W, pady=4, padx=(0, 8))
        default_price = item.get("sale_price") or item.get("sp") or ""
        self.price_var = tk.StringVar(value=str(default_price) if default_price else "")
        price_entry = ttk.Entry(container, textvariable=self.price_var)
        price_entry.grid(row=3, column=1, sticky="ew", pady=4)

        ttk.Label(container, text="Notes:").grid(row=4, column=0, sticky=tk.NW, pady=4, padx=(0, 8))
        self.note_text = tk.Text(container, height=4, width=40)
        self.note_text.grid(row=4, column=1, sticky="ew", pady=4)
        if item.get("sale_note"):
            self.note_text.insert("1.0", str(item.get("sale_note")))

        button_row = ttk.Frame(container)
        button_row.grid(row=5, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(button_row, text="Cancel", command=self.window.destroy).pack(side=tk.RIGHT)
        ttk.Button(
            button_row,
            text="Mark Sold",
            style="Accent.TButton",
            command=self._mark_sold,
        ).pack(side=tk.RIGHT, padx=(0, 8))

        self._load_customers(select_id=item.get("customer_id"))

    def _load_customers(self, select_id: Optional[Any] = None) -> None:
        try:
            customers = db.fetch_customers()
        except Exception as exc:
            messagebox.showerror("Mark as Sold", f"Failed to load customers: {exc}")
            customers = []
        self.customer_options = customers
        display_values = []
        selection_index = -1
        for index, record in enumerate(customers):
            name = record.get("full_name") or "Unnamed"
            phone = record.get("phone") or record.get("email") or ""
            label = name if not phone else f"{name} — {phone}"
            display_values.append(label)
            if select_id is not None and record.get("id") == select_id:
                selection_index = index
        self.customer_combo["values"] = display_values
        if selection_index >= 0:
            self.customer_combo.current(selection_index)
        elif display_values:
            self.customer_combo.current(0)

    def _open_new_customer(self) -> None:
        CustomerDialog(
            self.window,
            customer=None,
            on_save=self._handle_new_customer,
        )

    def _handle_new_customer(self, record: Dict[str, Any]) -> None:
        if self.on_customer_created:
            self.on_customer_created(record)
        self._load_customers(select_id=record.get("id"))

    def _selected_customer_id(self) -> Optional[str]:
        index = self.customer_combo.current()
        if index < 0 or index >= len(self.customer_options):
            return None
        try:
            return str(self.customer_options[index]["id"])
        except (TypeError, ValueError, KeyError):
            return None

    def _mark_sold(self) -> None:
        customer_id = self._selected_customer_id()
        if customer_id is None:
            messagebox.showerror("Mark as Sold", "Please select a customer.")
            return

        price_value = self.price_var.get().strip()
        note_value = self.note_text.get("1.0", tk.END).strip()

        try:
            db.mark_item_sold(
                self.item["item_id"],
                customer_id=customer_id,
                sale_price=price_value if price_value else None,
                note=note_value,
            )
        except ValueError as exc:
            messagebox.showerror("Mark as Sold", str(exc))
            return
        except Exception as exc:
            messagebox.showerror("Mark as Sold", f"Unable to mark item as sold: {exc}")
            return

        messagebox.showinfo("Mark as Sold", "Item marked as sold successfully.")
        if self.on_complete:
            self.on_complete()
        self.window.destroy()

