import tkinter as tk
from tkinter import ttk, messagebox
from typing import Optional

import db
from ui_item_card import ItemCardWindow


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

        self.table_frame = ttk.Frame(self.root, padding=(10, 0, 10, 10))
        self.table_frame.pack(fill=tk.BOTH, expand=True)

        columns = (
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

        self.tree = ttk.Treeview(self.table_frame, columns=columns, show="headings", height=15)
        for col in columns:
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

        self.open_button = ttk.Button(self.button_frame, text="Open Item", command=self.on_open_item)
        self.open_button.pack(anchor=tk.E)

        self.tree.bind("<Double-1>", self.on_tree_double_click)

    def load_items(self) -> None:
        collection_filter = self.collection_var.get().strip() or None
        brand_filter = self.brand_var.get().strip() or None
        status_filter = self.status_var.get().strip() or None

        for row in self.tree.get_children():
            self.tree.delete(row)

        items = db.fetch_items(collection_filter, brand_filter, status_filter)
        for item in items:
            self.tree.insert(
                "",
                tk.END,
                iid=item["item_id"],
                values=(
                    item["rug_no"],
                    item["sku"],
                    item["collection"],
                    item["brand"],
                    item["design"],
                    item["size_label"],
                    f"{item['area']:.2f}" if item["area"] is not None else "",
                    item["stock_location"],
                    item["status"],
                ),
            )

    def on_search(self) -> None:
        self.load_items()

    def on_tree_double_click(self, event: tk.Event) -> None:
        self.open_selected_item()

    def on_open_item(self) -> None:
        self.open_selected_item()

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
