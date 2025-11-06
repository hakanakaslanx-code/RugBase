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

        self.form_fields = list(db.MASTER_SHEET_COLUMNS)
        self.label_map = {field: label for field, label in self.form_fields}
        self.all_fields = ["item_id", *db.UPDATABLE_FIELDS, "created_at", "updated_at"]

        self.vars: Dict[str, tk.StringVar] = {}
        self._loading = False
        self._suspend_area_update = False
        self._area_user_override = False

        self._create_widgets()
        self._bind_traces()
        self._load_item()

    def _create_widgets(self) -> None:
        container = ttk.Frame(self.window, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        # Item identifier row
        self.vars["item_id"] = tk.StringVar()
        id_label = ttk.Label(container, text="Item ID:")
        id_label.grid(row=0, column=0, sticky=tk.W, pady=3, padx=(0, 5))
        id_entry = ttk.Entry(container, textvariable=self.vars["item_id"])
        id_entry.grid(row=0, column=1, columnspan=3, sticky="we", pady=3)
        if not self.is_new:
            id_entry.configure(state="readonly")

        columns_per_row = 2
        start_row = 1
        for index, (field, label_text) in enumerate(self.form_fields):
            row = start_row + index // columns_per_row
            col = (index % columns_per_row) * 2
            label = ttk.Label(container, text=f"{label_text}:")
            label.grid(row=row, column=col, sticky=tk.W, pady=3, padx=(0, 5))

            var = tk.StringVar()
            entry = ttk.Entry(container, textvariable=var)
            entry.grid(row=row, column=col + 1, sticky="we", pady=3, padx=(0, 15))
            if field == "area":
                entry.configure(takefocus=True)

            self.vars[field] = var

        meta_start = start_row + (len(self.form_fields) + columns_per_row - 1) // columns_per_row
        for offset, (field, label_text) in enumerate((("created_at", "Created At"), ("updated_at", "Updated At"))):
            self.vars[field] = tk.StringVar()
            label = ttk.Label(container, text=f"{label_text}:")
            label.grid(row=meta_start + offset, column=0, sticky=tk.W, pady=3, padx=(0, 5))
            entry = ttk.Entry(container, textvariable=self.vars[field], state="readonly")
            entry.grid(row=meta_start + offset, column=1, columnspan=3, sticky="we", pady=3)

        save_button = ttk.Button(container, text="Save", command=self._on_save)
        save_button.grid(row=meta_start + 2, column=0, columnspan=4, pady=(10, 0))

        container.columnconfigure(1, weight=1)
        container.columnconfigure(3, weight=1)

    def _load_item(self) -> None:
        self._loading = True
        if self.is_new:
            generated_id = db.generate_item_id()
            self.vars["item_id"].set(generated_id)
            self._loading = False
            self._area_user_override = False
            self._on_dimensions_change()
            return

        item = db.fetch_item(self.item_id)
        if not item:
            messagebox.showerror("Item Not Found", "The selected item could not be found.")
            self.window.destroy()
            return

        for field in self.all_fields:
            var = self.vars.get(field)
            if not var:
                continue
            value = item.get(field)
            var.set("" if value is None else str(value))

        self._loading = False
        self._area_user_override = bool(self.vars.get("area", tk.StringVar()).get().strip())
        self._on_dimensions_change()

    def _on_save(self) -> None:
        raw_values = {field: self.vars[field].get().strip() for field in self.vars}
        item_id = raw_values.get("item_id", "")
        if not item_id:
            messagebox.showerror("Missing Item ID", "Item ID is required.")
            return

        if self.is_new and not (
            raw_values.get("rug_no")
            or raw_values.get("upc")
            or raw_values.get("roll_no")
        ):
            messagebox.showerror(
                "Missing Required Fields",
                "Please provide at least a RugNo, UPC, or RollNo before saving.",
            )
            return

        item_data: Dict[str, object] = {"item_id": item_id}
        numeric_fields = set(db.NUMERIC_FIELDS) - {"area"}

        for field in db.UPDATABLE_FIELDS:
            if field == "area":
                continue
            value = raw_values.get(field, "")
            if field in numeric_fields:
                if value:
                    parsed = db.parse_numeric(value)
                    if parsed is None:
                        field_label = self.label_map.get(field, field.replace("_", " ").title())
                        messagebox.showerror("Invalid Value", f"{field_label} must be numeric.")
                        return
                    item_data[field] = parsed
                else:
                    item_data[field] = None
            else:
                item_data[field] = value or None

        area_input = raw_values.get("area", "")
        computed_area = db.calculate_area(
            raw_values.get("st_size"),
            area_input if area_input else None,
            raw_values.get("a_size"),
        )
        if area_input and computed_area is None:
            messagebox.showerror(
                "Invalid Area",
                "Area must be numeric or derivable from StSize.",
            )
            return
        item_data["area"] = computed_area

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

    def _bind_traces(self) -> None:
        area_var = self.vars.get("area")
        if area_var is not None:
            area_var.trace_add("write", self._on_area_manual_change)
        for key in ("st_size", "a_size"):
            var = self.vars.get(key)
            if var is not None:
                var.trace_add("write", self._on_dimensions_change)

    def _on_area_manual_change(self, *_: object) -> None:
        if self._loading or self._suspend_area_update:
            return
        area_value = self.vars.get("area")
        if area_value is None:
            return
        self._area_user_override = bool(area_value.get().strip())

    def _on_dimensions_change(self, *_: object) -> None:
        if self._loading or self._area_user_override:
            return

        st_size_var = self.vars.get("st_size")
        a_size_var = self.vars.get("a_size")
        area_var = self.vars.get("area")
        if area_var is None:
            return

        computed_area = db.calculate_area(
            st_size_var.get() if st_size_var else None,
            None,
            a_size_var.get() if a_size_var else None,
        )
        if computed_area is None:
            return

        self._suspend_area_update = True
        area_var.set(f"{computed_area:.2f}")
        self._suspend_area_update = False
