import csv
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Optional

import db
from core import importer, updater
from core.version import __version__
from ui_item_card import ItemCardWindow
from core.excel import Workbook


class MainWindow:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self._create_widgets()
        self.load_items()

    def _create_widgets(self) -> None:
        self.filter_frame = ttk.Frame(self.root, padding=10)
        self.filter_frame.pack(fill=tk.X)

        ttk.Label(self.filter_frame, text="Collection:").grid(row=0, column=0, padx=(0, 5), pady=5, sticky=tk.W)
        self.collection_var = tk.StringVar()
        ttk.Entry(self.filter_frame, textvariable=self.collection_var, width=20).grid(row=0, column=1, pady=5, sticky=tk.W)

        ttk.Label(self.filter_frame, text="Brand:").grid(row=0, column=2, padx=(15, 5), pady=5, sticky=tk.W)
        self.brand_var = tk.StringVar()
        ttk.Entry(self.filter_frame, textvariable=self.brand_var, width=20).grid(row=0, column=3, pady=5, sticky=tk.W)

        ttk.Label(self.filter_frame, text="Status:").grid(row=0, column=4, padx=(15, 5), pady=5, sticky=tk.W)
        self.status_var = tk.StringVar()
        ttk.Entry(self.filter_frame, textvariable=self.status_var, width=20).grid(row=0, column=5, pady=5, sticky=tk.W)

        self.search_button = ttk.Button(self.filter_frame, text="Search", command=self.on_search)
        self.search_button.grid(row=0, column=6, padx=(15, 0), pady=5, sticky=tk.W)

        self.filter_frame.columnconfigure(7, weight=1)

        self.export_frame = ttk.Frame(self.root, padding=(10, 0, 10, 5))
        self.export_frame.pack(fill=tk.X)

        self.import_csv_button = ttk.Button(self.export_frame, text="Import CSV", command=self.on_import_csv)
        self.import_csv_button.pack(side=tk.LEFT)

        self.import_xml_button = ttk.Button(self.export_frame, text="Import XML", command=self.on_import_xml)
        self.import_xml_button.pack(side=tk.LEFT, padx=(10, 0))

        self.export_csv_button = ttk.Button(self.export_frame, text="Export CSV", command=self.on_export_csv)
        self.export_csv_button.pack(side=tk.LEFT, padx=(10, 0))

        self.export_xlsx_button = ttk.Button(self.export_frame, text="Export XLSX", command=self.on_export_xlsx)
        self.export_xlsx_button.pack(side=tk.LEFT, padx=(10, 0))

        self.table_frame = ttk.Frame(self.root, padding=(10, 0, 10, 10))
        self.table_frame.pack(fill=tk.BOTH, expand=True)

        self.columns = (
            "rug_no",
            "sku",
            "collection",
            "brand",
            "design",
            "size_label",
            "area",
            "stock_location",
            "status",
        )

        self.tree = ttk.Treeview(self.table_frame, columns=self.columns, show="headings", height=15)
        for col in self.columns:
            self.tree.heading(col, text=col.replace("_", " ").title())
            self.tree.column(col, anchor=tk.W, width=120)
        self.tree.column("design", width=160)
        self.tree.column("area", anchor=tk.E, width=80)

        scrollbar = ttk.Scrollbar(self.table_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")

        self.table_frame.rowconfigure(0, weight=1)
        self.table_frame.columnconfigure(0, weight=1)

        self.button_frame = ttk.Frame(self.root, padding=(10, 0, 10, 10))
        self.button_frame.pack(fill=tk.X)

        self.add_button = ttk.Button(self.button_frame, text="Add Item", command=self.on_add_item)
        self.add_button.pack(side=tk.LEFT)

        self.delete_button = ttk.Button(
            self.button_frame, text="Delete Item", command=self.on_delete_item
        )
        self.delete_button.pack(side=tk.LEFT, padx=(10, 0))

        self.open_button = ttk.Button(self.button_frame, text="Open Item", command=self.on_open_item)
        self.open_button.pack(side=tk.RIGHT)

        self.tree.bind("<Double-1>", self.on_tree_double_click)

        self.footer_frame = ttk.Frame(self.root, padding=(10, 0, 10, 10))
        self.footer_frame.pack(fill=tk.X)

        self.totals_var = tk.StringVar(value="Total items: 0    Total area: 0.00")
        self.totals_label = ttk.Label(self.footer_frame, textvariable=self.totals_var)
        self.totals_label.pack(side=tk.LEFT)

        self.update_button = ttk.Button(self.footer_frame, text="Check for Updates", command=self.on_check_for_updates)
        self.update_button.pack(side=tk.RIGHT)

        self.version_label = ttk.Label(self.footer_frame, text=f"Version {__version__}")
        self.version_label.pack(side=tk.RIGHT, padx=(0, 10))

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

    def on_search(self) -> None:
        self.load_items()

    def on_tree_double_click(self, event: tk.Event) -> None:
        self.open_selected_item()

    def on_open_item(self) -> None:
        self.open_selected_item()

    def on_add_item(self) -> None:
        ItemCardWindow(self.root, None, on_save=self.load_items)

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
        collection_filter = self.collection_var.get().strip() or None
        brand_filter = self.brand_var.get().strip() or None
        status_filter = self.status_var.get().strip() or None

        return db.fetch_items(collection_filter, brand_filter, status_filter)

    def _format_item_values(self, item: dict) -> tuple[str, ...]:
        values = [
            item["rug_no"],
            item["sku"],
            item["collection"],
            item["brand"],
            item["design"],
            item["size_label"],
            f"{item['area']:.2f}" if item["area"] is not None else "",
            item["stock_location"],
            item["status"],
        ]
        return tuple(values)

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
                writer.writerow([self.tree.heading(col)["text"] for col in self.columns])
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
        sheet.append([self.tree.heading(col)["text"] for col in self.columns])
        for item in items:
            sheet.append(self._format_item_values(item))

        try:
            workbook.save(file_path)
        except OSError as exc:
            messagebox.showerror("Export XLSX", f"Failed to export XLSX: {exc}")

    def on_check_for_updates(self) -> None:
        """Prompt the user to download the latest RugBase release."""

        updater.prompt_for_update(self.root)

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
