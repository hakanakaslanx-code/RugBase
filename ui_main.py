import csv
import os
from datetime import datetime
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter import font as tkfont
from typing import Optional

import db
from core import importer, updater
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
        self._configure_style()
        self._create_widgets()
        self.load_items()
        self.root.bind("<Control-l>", self.on_open_label_generator)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.minsize(1024, 640)

    def _configure_style(self) -> None:
        try:
            self.style.theme_use("clam")
        except tk.TclError:
            pass
        self.style.configure("Header.TLabel", font=("Segoe UI", 18, "bold"))
        self.style.configure("SubHeader.TLabel", font=("Segoe UI", 10), foreground="#555555")
        self.style.configure("Accent.TButton", font=("Segoe UI", 10, "bold"))
        self.style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))

    def _create_widgets(self) -> None:
        self._build_menu()
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        self.dashboard_container = ScrollableFrame(self.notebook, padding=12)
        self.dashboard_frame = self.dashboard_container.content
        self.dashboard_frame.columnconfigure(0, weight=1)
        self.notebook.add(self.dashboard_container, text="Dashboard")

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
            foreground="#555555",
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

    def on_open_item(self) -> None:
        self.open_selected_item()

    def on_add_item(self) -> None:
        ItemCardWindow(self.root, None, on_save=self.load_items)

    def open_label_generator(self) -> None:
        if self.label_window and self.label_window.window.winfo_exists():
            self.label_window.window.focus_set()
            return
        self.label_window = LabelGeneratorWindow(self.root, on_close=self._clear_label_window)

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

        return db.fetch_items(rug_no_filter, collection_filter, brand_filter, style_filter)

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

        window = tk.Toplevel(self.root)
        window.title("RugBase Debug Log")
        window.geometry("720x480")
        window.transient(self.root)

        text_widget = tk.Text(window, wrap="none")
        text_widget.insert("1.0", content or "(Log file is empty)")
        try:
            fixed_font = tkfont.nametofont("TkFixedFont")
        except tk.TclError:
            fixed_font = ("Consolas", 10)
        text_widget.configure(state="disabled", font=fixed_font)

        yscroll = ttk.Scrollbar(window, orient="vertical", command=text_widget.yview)
        xscroll = ttk.Scrollbar(window, orient="horizontal", command=text_widget.xview)
        text_widget.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        text_widget.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        window.rowconfigure(0, weight=1)
        window.columnconfigure(0, weight=1)

        ttk.Label(window, text=str(log_path), foreground="#555555").grid(
            row=2, column=0, columnspan=2, sticky="w", padx=8, pady=(6, 10)
        )

    def _on_close(self) -> None:
        if hasattr(self, "sync_panel"):
            try:
                self.sync_panel.shutdown()
            except Exception:  # pragma: no cover - defensive cleanup
                pass
        self.root.destroy()
