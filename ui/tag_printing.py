"""Tkinter window for generating rug tag PDFs."""
from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable, Iterable, List, Optional

import db
from core.labels import create_tag_labels_pdf, open_pdf


class TagPrintingWindow:
    def __init__(self, master: tk.Misc, on_close: Optional[Callable[[], None]] = None) -> None:
        self.master = master
        self.on_close = on_close
        self.window = tk.Toplevel(master)
        self.window.title("Etiket Basımı")
        self.window.resizable(False, False)
        self.window.transient(master)
        self.window.grab_set()

        self._items: List[dict] = []

        self._build_ui()
        self.window.protocol("WM_DELETE_WINDOW", self.close)

    def _build_ui(self) -> None:
        container = ttk.Frame(self.window, padding=15)
        container.grid(row=0, column=0, sticky="nsew")

        ttk.Label(container, text="Halı No (virgülle ayrılmış)").grid(row=0, column=0, sticky=tk.W)
        self.rug_numbers_var = tk.StringVar()
        self.rug_entry = ttk.Entry(container, textvariable=self.rug_numbers_var, width=40)
        self.rug_entry.grid(row=1, column=0, pady=(2, 10), sticky=tk.EW)
        self.rug_entry.insert(0, "38564, 38566")

        button_frame = ttk.Frame(container)
        button_frame.grid(row=2, column=0, pady=(0, 10), sticky=tk.W)

        self.list_button = ttk.Button(button_frame, text="Listele", command=self.on_list)
        self.list_button.pack(side=tk.LEFT)

        self.print_button = ttk.Button(button_frame, text="Etiket Yazdır", command=self.on_print)
        self.print_button.pack(side=tk.LEFT, padx=(10, 0))

        self.close_button = ttk.Button(button_frame, text="Kapat", command=self.close)
        self.close_button.pack(side=tk.LEFT, padx=(10, 0))

        self.count_var = tk.StringVar(value="Bulunan Ürün: 0")
        ttk.Label(container, textvariable=self.count_var).grid(row=3, column=0, sticky=tk.W)

        columns = ("rug_no", "collection", "design", "size_label")
        self.tree = ttk.Treeview(container, columns=columns, show="headings", height=6)
        for col in columns:
            heading = col.replace("_", " ").title()
            self.tree.heading(col, text=heading)
            self.tree.column(col, anchor=tk.W, width=120)
        self.tree.grid(row=4, column=0, pady=(8, 0), sticky="nsew")

        container.columnconfigure(0, weight=1)
        container.rowconfigure(4, weight=1)

    def close(self) -> None:
        if self.on_close:
            self.on_close()
        self.window.grab_release()
        self.window.destroy()

    def _parse_rug_numbers(self) -> List[str]:
        raw_value = self.rug_numbers_var.get()
        parts = raw_value.replace("\n", ",").split(",")
        return [part.strip() for part in parts if part.strip()]

    def on_list(self) -> None:
        rug_numbers = self._parse_rug_numbers()
        if not rug_numbers:
            messagebox.showinfo("Etiket Basımı", "Lütfen en az bir halı numarası girin.")
            return

        try:
            items = db.fetch_items_by_rug_numbers(rug_numbers)
        except Exception as exc:
            messagebox.showerror("Etiket Basımı", f"Kayıtlar alınırken hata oluştu: {exc}")
            return

        self._items = items
        self._refresh_tree(items)
        self.count_var.set(f"Bulunan Ürün: {len(items)}")

        missing = self._find_missing_rug_numbers(rug_numbers, items)
        if missing:
            messagebox.showwarning(
                "Etiket Basımı",
                "Aşağıdaki halılar bulunamadı:\n" + ", ".join(missing),
            )
        elif not items:
            messagebox.showinfo("Etiket Basımı", "Girilen halılara ait kayıt bulunamadı.")

    def _refresh_tree(self, items: Iterable[dict]) -> None:
        for row in self.tree.get_children():
            self.tree.delete(row)
        for item in items:
            values = (
                item.get("rug_no", ""),
                item.get("collection", ""),
                item.get("design", ""),
                item.get("size_label", ""),
            )
            self.tree.insert("", tk.END, values=values)

    @staticmethod
    def _find_missing_rug_numbers(input_numbers: List[str], items: Iterable[dict]) -> List[str]:
        found = {str(item.get("rug_no")) for item in items if item.get("rug_no")}
        return [num for num in input_numbers if num not in found]

    def on_print(self) -> None:
        if not self._items:
            messagebox.showinfo(
                "Etiket Basımı",
                "Önce [Listele] tuşu ile yazdırılacak halıları getirin.",
            )
            return

        try:
            pdf_path = create_tag_labels_pdf(self._items)
            open_pdf(pdf_path)
        except Exception as exc:
            messagebox.showerror("Etiket Basımı", f"PDF oluşturulurken hata oluştu: {exc}")
            return

        messagebox.showinfo("Etiket Basımı", "Etiket PDF'i oluşturuldu ve açıldı.")
