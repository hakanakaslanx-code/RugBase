import os
import tempfile
import webbrowser
from pathlib import Path
from typing import Sequence

import tkinter as tk
from tkinter import messagebox, ttk

import db
from tag_templates import generate_dymo_2x4_vertical_pdf


DYMO_VERTICAL_TEMPLATE = "Dymo 2x4 Vertical (Rug #, MSRP)"


class TagPrintingDialog:
    def __init__(self, parent: tk.Tk, item_ids: Sequence[str]) -> None:
        self.parent = parent
        self.item_ids = list(item_ids)
        self.window = tk.Toplevel(parent)
        self.window.title("Tag Printing")
        self.window.transient(parent)
        self.window.grab_set()

        self.tag_type_var = tk.StringVar(value=DYMO_VERTICAL_TEMPLATE)

        self._build_ui()

    def _build_ui(self) -> None:
        container = ttk.Frame(self.window, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        ttk.Label(container, text=f"Selected items: {len(self.item_ids)}").grid(
            row=0, column=0, columnspan=2, sticky=tk.W, pady=(0, 10)
        )

        ttk.Label(container, text="Tag Type:").grid(row=1, column=0, sticky=tk.W, padx=(0, 10))
        tag_type_combo = ttk.Combobox(
            container,
            textvariable=self.tag_type_var,
            values=[DYMO_VERTICAL_TEMPLATE],
            state="readonly",
            width=40,
        )
        tag_type_combo.grid(row=1, column=1, sticky=tk.W)

        button_frame = ttk.Frame(container)
        button_frame.grid(row=2, column=0, columnspan=2, pady=(15, 0), sticky=tk.E)

        print_button = ttk.Button(button_frame, text="Print", command=self._on_print)
        print_button.pack(side=tk.LEFT, padx=(0, 10))

        cancel_button = ttk.Button(button_frame, text="Cancel", command=self.window.destroy)
        cancel_button.pack(side=tk.LEFT)

        container.columnconfigure(1, weight=1)

    def _on_print(self) -> None:
        tag_type = self.tag_type_var.get()
        if tag_type != DYMO_VERTICAL_TEMPLATE:
            messagebox.showerror("Tag Printing", "Unsupported tag type selected.")
            return

        items = db.fetch_items_by_ids(self.item_ids)
        if not items:
            messagebox.showinfo("Tag Printing", "No items were found for the selected IDs.")
            return

        try:
            pdf_path = self._generate_pdf(tag_type, items)
        except Exception as exc:  # pragma: no cover - GUI error path
            messagebox.showerror("Tag Printing", f"Failed to generate tags: {exc}")
            return

        try:
            webbrowser.open(Path(pdf_path).as_uri())
        except Exception:  # pragma: no cover - opening may fail silently
            messagebox.showwarning(
                "Tag Printing",
                "Tags were generated, but the PDF could not be opened automatically.",
            )

        self.window.destroy()

    def _generate_pdf(self, tag_type: str, items: list[dict]) -> str:
        fd, path = tempfile.mkstemp(prefix="tag-print-", suffix=".pdf")
        os.close(fd)

        if tag_type == DYMO_VERTICAL_TEMPLATE:
            generate_dymo_2x4_vertical_pdf(items, path)
        else:  # pragma: no cover - guarded by earlier check
            raise ValueError(f"Unsupported tag type: {tag_type}")

        return path
