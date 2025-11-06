import tkinter as tk
from tkinter import ttk, messagebox
from typing import Callable, Dict, Optional

import db


class ItemCardWindow:
    def __init__(self, parent: tk.Tk, item_id: str, on_save: Optional[Callable[[], None]] = None) -> None:
        self.parent = parent
        self.item_id = item_id
        self.on_save = on_save
        self.window = tk.Toplevel(parent)
        self.window.title(f"Item Card - {item_id}")
        self.window.transient(parent)
        self.window.grab_set()

        self.fields = [
            "item_id",
            "rug_no",
            "sku",
            "type",
            "collection",
            "brand",
            "v_design",
            "design",
            "ground",
            "border",
            "size_label",
            "st_size",
            "area",
            "stock_location",
            "godown",
            "purchase_date",
            "pv_no",
            "vendor",
            "sold_on",
            "invoice_no",
            "customer",
            "status",
            "payment_status",
            "notes",
            "created_at",
            "updated_at",
        ]

        self.vars: Dict[str, tk.StringVar] = {}

        self._create_widgets()
        self._load_item()

    def _create_widgets(self) -> None:
        container = ttk.Frame(self.window, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        for idx, field in enumerate(self.fields):
            label = ttk.Label(container, text=field.replace("_", " ").title() + ":")
            label.grid(row=idx, column=0, sticky=tk.W, pady=3)

            var = tk.StringVar()
            entry = ttk.Entry(container, textvariable=var, width=40)
            entry.grid(row=idx, column=1, sticky=tk.W, pady=3)

            if field in {"item_id", "created_at", "updated_at"}:
                entry.configure(state="readonly")

            self.vars[field] = var

        save_button = ttk.Button(container, text="Save", command=self._on_save)
        save_button.grid(row=len(self.fields), column=0, columnspan=2, pady=(10, 0))

        container.columnconfigure(1, weight=1)

    def _load_item(self) -> None:
        item = db.fetch_item(self.item_id)
        if not item:
            messagebox.showerror("Item Not Found", "The selected item could not be found.")
            self.window.destroy()
            return

        for field in self.fields:
            value = item.get(field)
            if value is None:
                self.vars[field].set("")
            else:
                self.vars[field].set(str(value))

    def _on_save(self) -> None:
        item_data = {field: self.vars[field].get().strip() for field in self.fields}
        for field, value in list(item_data.items()):
            if field in {"item_id", "area"}:
                continue
            if value == "":
                item_data[field] = None
        area_value = item_data.get("area")
        if area_value:
            try:
                item_data["area"] = float(area_value)
            except ValueError:
                messagebox.showerror("Invalid Area", "Area must be a number.")
                return
        else:
            item_data["area"] = None

        db.update_item(item_data)
        messagebox.showinfo("Item Saved", "Item details have been updated.")

        if self.on_save:
            self.on_save()

        self.window.destroy()
