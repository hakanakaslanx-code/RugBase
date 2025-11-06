import sqlite3
import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable, Dict, Optional

import db


class ItemCardWindow:
    def __init__(
        self,
        parent: tk.Tk,
        item_id: Optional[str] = None,
        on_save: Optional[Callable[[], None]] = None,
    ) -> None:
        self.parent = parent
        self.item_id = item_id
        self.is_new = item_id is None
        self.on_save = on_save
        self.window = tk.Toplevel(parent)
        title_id = item_id if item_id is not None else "New Item"
        self.window.title(f"Item Card - {title_id}")
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

            if field in {"created_at", "updated_at"}:
                entry.configure(state="readonly")
            elif field == "item_id" and not self.is_new:
                entry.configure(state="readonly")

            self.vars[field] = var

        save_button = ttk.Button(container, text="Save", command=self._on_save)
        save_button.grid(row=len(self.fields), column=0, columnspan=2, pady=(10, 0))

        container.columnconfigure(1, weight=1)

    def _load_item(self) -> None:
        if self.is_new:
            generated_id = db.generate_item_id()
            self.vars["item_id"].set(generated_id)
            self.vars["status"].set("In Stock")
            self.vars["payment_status"].set("Pending")
            return

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
        raw_values = {field: self.vars[field].get().strip() for field in self.fields}
        item_id = raw_values.get("item_id", "")
        if not item_id:
            messagebox.showerror("Missing Item ID", "Item ID is required.")
            return

        if self.is_new and not (raw_values.get("rug_no") or raw_values.get("sku")):
            messagebox.showerror(
                "Missing Required Fields",
                "Please provide at least a Rug No or SKU before saving.",
            )
            return

        item_data: Dict[str, object] = {"item_id": item_id}
        for field in db.UPDATABLE_FIELDS:
            value = raw_values.get(field, "")
            if field == "area":
                if value:
                    try:
                        item_data[field] = float(value)
                    except ValueError:
                        messagebox.showerror("Invalid Area", "Area must be a number.")
                        return
                else:
                    item_data[field] = None
            else:
                item_data[field] = value or None

        try:
            if self.is_new:
                new_id = db.insert_item(item_data)
                messagebox.showinfo("Item Saved", f"New item {new_id} has been created.")
            else:
                db.update_item(item_data)
                messagebox.showinfo("Item Saved", "Item details have been updated.")
        except sqlite3.IntegrityError as exc:
            messagebox.showerror("Save Failed", f"Could not save the item: {exc}")
            return

        if self.on_save:
            self.on_save()

        self.window.destroy()
