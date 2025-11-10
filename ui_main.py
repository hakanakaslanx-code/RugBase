import csv
import os
import subprocess
import sys
from datetime import datetime
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter import font as tkfont
from typing import Any, Callable, Dict, List, Optional

import db
from core import app_paths, importer, updater
from core.logging_config import get_log_path
from core.version import __version__
from ui_item_card import ItemCardWindow
from ui_label_generator import LabelGeneratorWindow
from core.excel import Workbook
from consignment_ui import ConsignmentListWindow, ConsignmentModal, ReturnModal
from ui.sync_panel import SyncPanel


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
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.label_window: Optional[LabelGeneratorWindow] = None
        self.current_user = os.getenv("USERNAME") or os.getenv("USER") or "operator"
        self.style = ttk.Style(self.root)
        self.dark_mode_var = tk.BooleanVar(value=False)
        self._palette: dict[str, str] = {}
        self.customer_form_vars: Dict[str, tk.StringVar] = {}
        self.customer_notes_text: Optional[tk.Text] = None
        self.customer_tree: Optional[ttk.Treeview] = None
        self.customer_records: Dict[int, Dict[str, Any]] = {}
        self.sales_period_days: Optional[int] = 30
        self._configure_style()
        self._create_widgets()
        self.load_items()
        self.load_customers()
        self.root.bind("<Control-l>", self.on_open_label_generator)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.minsize(1024, 640)
        self._apply_theme()

    def _configure_style(self) -> None:
        try:
            self.style.theme_use("clam")
        except tk.TclError:
            pass
        self.style.configure("Header.TLabel", font=("Segoe UI", 18, "bold"))
        self.style.configure("SubHeader.TLabel", font=("Segoe UI", 10))
        self.style.configure("Accent.TButton", font=("Segoe UI", 10, "bold"))
        self.style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))

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

        if hasattr(self, "dashboard_container"):
            self.dashboard_container._canvas.configure(
                background=palette["background"],
                highlightbackground=palette["border"],
            )

        if hasattr(self, "sync_panel"):
            self.sync_panel.apply_theme(palette)

        if self.label_window and self.label_window.window.winfo_exists():
            self.label_window.apply_theme(palette)

    def _create_widgets(self) -> None:
        self._build_menu()
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        self.dashboard_container = ScrollableFrame(self.notebook, padding=12)
        self.dashboard_frame = self.dashboard_container.content
        self.dashboard_frame.columnconfigure(0, weight=1)
        self.notebook.add(self.dashboard_container, text="Dashboard")

        customers_tab = ttk.Frame(self.notebook, padding=12)
        self.notebook.add(customers_tab, text="Customers")
        self._build_customers_tab(customers_tab)

        sync_tab = ttk.Frame(self.notebook, padding=12)
        self.notebook.add(sync_tab, text="Sync")
        self.sync_panel = SyncPanel(sync_tab)
        self.sync_panel.pack(fill=tk.BOTH, expand=True)

        header_frame = ttk.Frame(self.dashboard_frame, padding=(10, 10))
        header_frame.pack(fill=tk.X)
        ttk.Label(header_frame, text="RugBase Inventory Dashboard", style="Header.TLabel").pack(
            anchor=tk.W
        )
        ttk.Label(
            header_frame,
            text=f"Signed in as {self.current_user}",
            style="SubHeader.TLabel",
        ).pack(anchor=tk.W, pady=(2, 0))

        self.filter_frame = ttk.LabelFrame(self.dashboard_frame, text="Filters", padding=12)
        self.filter_frame.pack(fill=tk.X, pady=(5, 10))

        self.rug_no_var = tk.StringVar()
        self.collection_var = tk.StringVar()
        self.brand_var = tk.StringVar()
        self.style_var = tk.StringVar()

        fields = [
            ("Rug No", self.rug_no_var),
            ("Collection", self.collection_var),
            ("Brand", self.brand_var),
            ("Style", self.style_var),
        ]
        for index, (label, variable) in enumerate(fields):
            base_col = index * 2
            ttk.Label(self.filter_frame, text=f"{label}:").grid(
                row=0, column=base_col, padx=(0, 6), pady=4, sticky=tk.W
            )
            ttk.Entry(self.filter_frame, textvariable=variable, width=22).grid(
                row=0, column=base_col + 1, padx=(0, 16), pady=4, sticky=tk.W
            )

        spacer_col = len(fields) * 2
        self.filter_frame.columnconfigure(spacer_col, weight=1)
        self.search_button = ttk.Button(
            self.filter_frame, text="Search", style="Accent.TButton", command=self.on_search
        )
        self.search_button.grid(row=0, column=spacer_col + 1, padx=(0, 8), pady=4, sticky=tk.E)
        self.clear_button = ttk.Button(
            self.filter_frame, text="Clear Filters", command=self.on_clear_filters
        )
        self.clear_button.grid(row=0, column=spacer_col + 2, padx=(0, 0), pady=4, sticky=tk.E)
        self.show_sold_var = tk.BooleanVar(value=False)
        self.sold_filter_check = ttk.Checkbutton(
            self.filter_frame,
            text="Sold List",
            variable=self.show_sold_var,
            command=self.load_items,
        )
        self.sold_filter_check.grid(row=0, column=spacer_col + 3, padx=(12, 0), pady=4, sticky=tk.E)

        self.export_frame = ttk.LabelFrame(self.dashboard_frame, text="Import & Export", padding=12)
        self.export_frame.pack(fill=tk.X, pady=(0, 10))

        self.import_csv_button = ttk.Button(
            self.export_frame, text="Import CSV", command=self.on_import_csv
        )
        self.import_csv_button.pack(side=tk.LEFT)

        self.import_xml_button = ttk.Button(
            self.export_frame, text="Import XML", command=self.on_import_xml
        )
        self.import_xml_button.pack(side=tk.LEFT, padx=(10, 0))

        self.export_csv_button = ttk.Button(
            self.export_frame, text="Export CSV", command=self.on_export_csv
        )
        self.export_csv_button.pack(side=tk.LEFT, padx=(10, 0))

        self.export_xlsx_button = ttk.Button(
            self.export_frame, text="Export XLSX", command=self.on_export_xlsx
        )
        self.export_xlsx_button.pack(side=tk.LEFT, padx=(10, 0))

        ttk.Label(
            self.dashboard_frame,
            text=(
                "Google Drive senkronizasyonu kaldırıldı."
                " Excel/Sheets senkronizasyonunu kullanmak için Sync sekmesine geçebilirsiniz."
            ),
            style="Hint.TLabel",
            wraplength=720,
            justify=tk.LEFT,
        ).pack(fill=tk.X, pady=(0, 10))

        ttk.Separator(self.dashboard_frame).pack(fill=tk.X, pady=(0, 10))

        self.table_frame = ttk.Frame(self.dashboard_frame)
        self.table_frame.pack(fill=tk.BOTH, expand=True)

        self.column_defs = list(db.MASTER_SHEET_COLUMNS)
        self.columns = [field for field, _ in self.column_defs]

        self.tree = ttk.Treeview(self.table_frame, columns=self.columns, show="headings", height=18)
        for field, header in self.column_defs:
            anchor = tk.E if field in db.NUMERIC_FIELDS else tk.W
            self.tree.heading(field, text=header)
            self.tree.column(field, anchor=anchor, width=130, stretch=False)

        yscroll = ttk.Scrollbar(self.table_frame, orient=tk.VERTICAL, command=self.tree.yview)
        xscroll = ttk.Scrollbar(self.table_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        self.table_frame.rowconfigure(0, weight=1)
        self.table_frame.columnconfigure(0, weight=1)

        self.tree_menu = tk.Menu(self.tree, tearoff=0)
        self.tree_menu.add_command(label="Mark as Sold...", command=self.open_mark_sold_modal)
        self.tree.bind("<Button-3>", self.on_tree_right_click)

        self.grand_total_frame = ttk.Frame(self.dashboard_frame, padding=(10, 0))
        self.grand_total_frame.pack(fill=tk.X, pady=(8, 0))
        self.grand_total_var = tk.StringVar(value="Grand Total: $0.00")
        ttk.Label(
            self.grand_total_frame,
            textvariable=self.grand_total_var,
            style="SubHeader.TLabel",
        ).pack(side=tk.LEFT)

        ttk.Separator(self.dashboard_frame).pack(fill=tk.X, pady=(10, 10))

        self.button_frame = ttk.LabelFrame(self.dashboard_frame, text="Item Actions", padding=12)
        self.button_frame.pack(fill=tk.X)

        self.add_button = ttk.Button(
            self.button_frame, text="Add Item", style="Accent.TButton", command=self.on_add_item
        )
        self.add_button.pack(side=tk.LEFT)

        self.label_button = ttk.Button(
            self.button_frame,
            text="Generate Label (DYMO)",
            command=self.open_label_generator,
        )
        self.label_button.pack(side=tk.LEFT, padx=(10, 0))

        self.consignment_button = ttk.Button(
            self.button_frame,
            text="Consignment Out",
            command=self.open_consignment_modal,
        )
        self.consignment_button.pack(side=tk.LEFT, padx=(10, 0))

        self.return_button = ttk.Button(
            self.button_frame,
            text="Consignment Returns",
            command=self.open_return_modal,
        )
        self.return_button.pack(side=tk.LEFT, padx=(10, 0))

        self.delete_button = ttk.Button(
            self.button_frame, text="Delete Item", command=self.on_delete_item
        )
        self.delete_button.pack(side=tk.LEFT, padx=(10, 0))

        self.consignment_list_button = ttk.Button(
            self.button_frame,
            text="View Consignments",
            command=self.open_consignment_list,
        )
        self.consignment_list_button.pack(side=tk.LEFT, padx=(10, 0))

        self.open_button = ttk.Button(
            self.button_frame, text="Open Selected", command=self.on_open_item
        )
        self.open_button.pack(side=tk.RIGHT)

        self.tree.bind("<Double-1>", self.on_tree_double_click)

        self.sales_report_frame = ttk.LabelFrame(
            self.dashboard_frame, text="Sales Report", padding=12
        )
        self.sales_report_frame.pack(fill=tk.X, pady=(10, 0))

        range_frame = ttk.Frame(self.sales_report_frame)
        range_frame.pack(fill=tk.X)
        ttk.Label(range_frame, text="Range:").pack(side=tk.LEFT)
        ttk.Button(
            range_frame,
            text="30d",
            command=lambda: self.set_sales_report_period(30),
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            range_frame,
            text="90d",
            command=lambda: self.set_sales_report_period(90),
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            range_frame,
            text="All",
            command=lambda: self.set_sales_report_period(None),
        ).pack(side=tk.LEFT, padx=(6, 0))

        self.sales_report_var = tk.StringVar(value="Sales data not available.")
        ttk.Label(
            self.sales_report_frame,
            textvariable=self.sales_report_var,
            style="SubHeader.TLabel",
        ).pack(fill=tk.X, pady=(6, 0))

        self.footer_frame = ttk.Frame(self.dashboard_frame, padding=(10, 0, 10, 10))
        self.footer_frame.pack(fill=tk.X, pady=(10, 0))

        self.totals_var = tk.StringVar(value="Total items: 0    Total area: 0.00")
        self.totals_label = ttk.Label(self.footer_frame, textvariable=self.totals_var)
        self.totals_label.pack(side=tk.LEFT)

        self.update_button = ttk.Button(
            self.footer_frame, text="Check for Updates", command=self.on_check_for_updates
        )
        self.update_button.pack(side=tk.RIGHT)

        self.version_label = ttk.Label(self.footer_frame, text=f"Version {__version__}")
        self.version_label.pack(side=tk.RIGHT, padx=(0, 12))

    def load_items(self) -> None:
        for row in self.tree.get_children():
            self.tree.delete(row)

        items = self.get_filtered_rows()
        for item in items:
            self.tree.insert(
                "",
                tk.END,
                iid=item["item_id"],
                values=self._format_item_values(item),
            )

        self.update_totals(items)
        self.refresh_sales_report()
        self._autosize_columns()

    def on_search(self) -> None:
        self.load_items()

    def on_clear_filters(self) -> None:
        self.rug_no_var.set("")
        self.collection_var.set("")
        self.brand_var.set("")
        self.style_var.set("")
        self.load_items()

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
        ItemCardWindow(self.root, None, on_save=self.load_items)

    def open_label_generator(self) -> None:
        if self.label_window and self.label_window.window.winfo_exists():
            self.label_window.window.focus_set()
            return
        self.label_window = LabelGeneratorWindow(self.root, on_close=self._clear_label_window)
        self.label_window.apply_theme(self._palette)

    def on_open_label_generator(self, _event: tk.Event) -> None:
        self.open_label_generator()

    def _clear_label_window(self) -> None:
        self.label_window = None

    def open_consignment_modal(self) -> None:
        ConsignmentModal(self.root, self.current_user)

    def open_return_modal(self) -> None:
        ReturnModal(self.root, self.current_user)

    def open_consignment_list(self) -> None:
        ConsignmentListWindow(self.root)

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

    def _on_customer_created_from_sale(self, _record: Dict[str, Any]) -> None:
        self.load_customers()

    def _build_customers_tab(self, container: ttk.Frame) -> None:
        container.columnconfigure(0, weight=3)
        container.columnconfigure(1, weight=2)
        container.rowconfigure(0, weight=1)

        list_frame = ttk.LabelFrame(container, text="Customer List", padding=12)
        list_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
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
        form_frame.grid(row=0, column=1, sticky="nsew")
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
            customer_id = int(record.get("id")) if record.get("id") is not None else None
            if customer_id is None:
                continue
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
            self.customer_tree.insert("", tk.END, iid=str(customer_id), values=values)

    def on_customer_search(self) -> None:
        self.load_customers()

    def on_customer_clear(self) -> None:
        if hasattr(self, "customer_search_var"):
            self.customer_search_var.set("")
        self.load_customers()

    def get_selected_customer_id(self) -> Optional[int]:
        if not self.customer_tree:
            return None
        selected = self.customer_tree.selection()
        if not selected:
            return None
        try:
            return int(selected[0])
        except (TypeError, ValueError):
            return None

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

    def open_customer_dialog(self, customer_id: Optional[int] = None) -> None:
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
            self.customer_records[int(record["id"])] = record

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
            self.customer_records[int(customer_id)] = record

    def clear_customer_form(self) -> None:
        for variable in self.customer_form_vars.values():
            variable.set("")
        if self.customer_notes_text:
            self.customer_notes_text.delete("1.0", tk.END)


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
                customer_id = int(self.customer["id"])
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

    def _selected_customer_id(self) -> Optional[int]:
        index = self.customer_combo.current()
        if index < 0 or index >= len(self.customer_options):
            return None
        try:
            return int(self.customer_options[index]["id"])
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

    def get_filtered_rows(self) -> list[dict]:
        rug_no_filter = self.rug_no_var.get().strip() or None
        collection_filter = self.collection_var.get().strip() or None
        brand_filter = self.brand_var.get().strip() or None
        style_filter = self.style_var.get().strip() or None
        status_filter = "sold" if self.show_sold_var.get() else None

        return db.fetch_items(
            rug_no_filter,
            collection_filter,
            brand_filter,
            style_filter,
            status_filter,
        )

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
        total_area = sum((item["area"] or 0.0) for item in items)
        self.totals_var.set(f"Total items: {total_items}    Total area: {total_area:.2f}")

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

    def on_check_for_updates(self) -> None:
        """Prompt the user to download the latest RugBase release."""

        updater.check_for_updates(self.root)

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

    def _build_menu(self) -> None:
        menubar = tk.Menu(self.root)
        tools_menu = tk.Menu(menubar, tearoff=0)
        tools_menu.add_command(label="Open Debug Log", command=self.open_debug_log)
        tools_menu.add_command(label="Open Data Folder", command=self.open_data_folder)
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
                f"Klasör açılamadı: {exc}\n{path}",
                parent=self.root,
            )

    def _on_close(self) -> None:
        if hasattr(self, "sync_panel"):
            try:
                self.sync_panel.shutdown()
            except Exception:  # pragma: no cover - defensive cleanup
                pass
        self.root.destroy()
