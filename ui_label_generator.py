import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Callable, Dict, List, Optional

from PIL import ImageTk

import db
from label_renderer import DymoLabelRenderer


class LabelGeneratorWindow:
    def __init__(self, parent: tk.Tk, on_close: Optional[Callable[[], None]] = None) -> None:
        self.parent = parent
        self._on_close = on_close
        self.window = tk.Toplevel(parent)
        self.window.title("Etiket Üretici")
        self.window.geometry("940x600")
        self.window.transient(parent)
        self.window.grab_set()
        self.window.resizable(True, True)
        self.window.protocol("WM_DELETE_WINDOW", self.close)
        self.window.bind("<Escape>", lambda _: self.close())

        self.renderer = DymoLabelRenderer()

        self.rug_no_var = tk.StringVar()
        self.collection_var = tk.StringVar()
        self.design_var = tk.StringVar()
        self.color_var = tk.StringVar()
        self.size_var = tk.StringVar()
        self.origin_var = tk.StringVar()

        self.results: List[Dict[str, object]] = []
        self._result_index: Dict[str, Dict[str, object]] = {}
        self.preview_photo: Optional[ImageTk.PhotoImage] = None
        self.warning_var = tk.StringVar()

        self._build_layout()
        self._load_origin_values()

    # region UI setup
    def _build_layout(self) -> None:
        container = ttk.Frame(self.window, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        container.columnconfigure(0, weight=1)
        container.columnconfigure(1, weight=1)
        container.rowconfigure(1, weight=1)

        self._build_filters(container)
        self._build_results(container)
        self._build_preview(container)
        self._build_buttons(container)

    def _build_filters(self, container: ttk.Frame) -> None:
        filter_frame = ttk.LabelFrame(container, text="Filtreler", padding=10)
        filter_frame.grid(row=0, column=0, columnspan=2, sticky="nsew", pady=(0, 10))

        entries = [
            ("Rug No", self.rug_no_var),
            ("Collection", self.collection_var),
            ("Design", self.design_var),
            ("Color", self.color_var),
            ("Size", self.size_var),
        ]

        for index, (label, var) in enumerate(entries):
            ttk.Label(filter_frame, text=f"{label}:").grid(row=0, column=index * 2, padx=(0, 4), pady=4, sticky=tk.W)
            ttk.Entry(filter_frame, textvariable=var, width=18).grid(row=0, column=index * 2 + 1, padx=(0, 8), pady=4)

        ttk.Label(filter_frame, text="Origin:").grid(row=1, column=0, padx=(0, 4), pady=4, sticky=tk.W)
        self.origin_combo = ttk.Combobox(filter_frame, textvariable=self.origin_var, width=18)
        self.origin_combo.grid(row=1, column=1, padx=(0, 8), pady=4, sticky=tk.W)
        self.origin_combo.configure(postcommand=self._load_origin_values)

        search_button = ttk.Button(filter_frame, text="Ara", command=self.on_search)
        last_column = len(entries) * 2
        filter_frame.columnconfigure(last_column, weight=1)
        search_button.grid(row=1, column=last_column, padx=(0, 8), pady=4, sticky=tk.E)
        self.window.bind("<Return>", lambda _: self.on_search())

    def _build_results(self, container: ttk.Frame) -> None:
        results_frame = ttk.LabelFrame(container, text="Sonuçlar", padding=10)
        results_frame.grid(row=1, column=0, sticky="nsew")
        container.rowconfigure(1, weight=1)
        container.columnconfigure(0, weight=1)

        columns = ("rug_no", "collection", "design", "ground", "st_size")
        self.tree = ttk.Treeview(results_frame, columns=columns, show="headings", selectmode="extended")
        headers = {
            "rug_no": "Rug #",
            "collection": "Collection",
            "design": "Design",
            "ground": "Color",
            "st_size": "Size",
        }
        for column in columns:
            self.tree.heading(column, text=headers[column])
            self.tree.column(column, width=120, stretch=False)

        yscroll = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        results_frame.rowconfigure(0, weight=1)
        results_frame.columnconfigure(0, weight=1)

        self.tree.bind("<<TreeviewSelect>>", self.on_tree_select)

    def _build_preview(self, container: ttk.Frame) -> None:
        preview_frame = ttk.LabelFrame(container, text="Önizleme", padding=10)
        preview_frame.grid(row=1, column=1, sticky="nsew", padx=(10, 0))
        container.columnconfigure(1, weight=1)

        self.preview_label = ttk.Label(preview_frame)
        self.preview_label.pack(fill=tk.BOTH, expand=True)

        self.warning_label = ttk.Label(preview_frame, textvariable=self.warning_var, foreground="#a94442", wraplength=360)
        self.warning_label.pack(fill=tk.X, pady=(6, 0))

    def _build_buttons(self, container: ttk.Frame) -> None:
        button_frame = ttk.Frame(container)
        button_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        container.rowconfigure(2, weight=0)

        self.save_button = ttk.Button(button_frame, text="PDF Kaydet", command=self.on_save_pdf, state=tk.DISABLED)
        self.save_button.pack(side=tk.LEFT)

        self.bulk_button = ttk.Button(button_frame, text="Toplu PDF", command=self.on_save_bulk_pdf, state=tk.DISABLED)
        self.bulk_button.pack(side=tk.LEFT, padx=(8, 0))

        self.print_button = ttk.Button(button_frame, text="DYMO'ya Yazdır", command=self.on_print, state=tk.DISABLED)
        self.print_button.pack(side=tk.LEFT, padx=(8, 0))

        ttk.Button(button_frame, text="Kapat", command=self.close).pack(side=tk.RIGHT)

    # endregion

    def _load_origin_values(self) -> None:
        try:
            origins = db.fetch_distinct_values("origin")
        except Exception:
            origins = []
        self.origin_combo.configure(values=origins)

    # region data helpers
    def on_search(self) -> None:
        self.tree.delete(*self.tree.get_children())
        self.results = db.search_items_for_labels(
            rug_no=self.rug_no_var.get().strip() or None,
            collection=self.collection_var.get().strip() or None,
            design=self.design_var.get().strip() or None,
            color=self.color_var.get().strip() or None,
            size=self.size_var.get().strip() or None,
            origin=self.origin_var.get().strip() or None,
        )
        self._result_index = {item["item_id"]: item for item in self.results}
        for item in self.results:
            color = item.get("ground") or item.get("border") or ""
            values = (
                item.get("rug_no", ""),
                item.get("collection", ""),
                item.get("design", ""),
                color,
                item.get("st_size", ""),
            )
            self.tree.insert("", tk.END, iid=item["item_id"], values=values)

        if self.results:
            first_id = self.results[0]["item_id"]
            self.tree.selection_set(first_id)
            self.tree.focus(first_id)
            self.tree.see(first_id)
            self._update_buttons()
            self._update_preview()
        else:
            self._update_buttons()
            self._clear_preview()

    def _get_selected_items(self) -> List[Dict[str, object]]:
        selected = self.tree.selection()
        return [self._result_index[iid] for iid in selected if iid in self._result_index]

    def on_tree_select(self, _event: tk.Event) -> None:
        self._update_buttons()
        self._update_preview()

    def _update_buttons(self) -> None:
        selected_count = len(self.tree.selection())
        state_single = tk.NORMAL if selected_count == 1 else tk.DISABLED
        self.save_button.configure(state=state_single)
        self.print_button.configure(state=state_single)
        self.bulk_button.configure(state=tk.NORMAL if selected_count > 1 else tk.DISABLED)
        if selected_count == 0:
            self.warning_var.set("")

    def _clear_preview(self) -> None:
        self.preview_label.configure(image="")
        self.preview_photo = None
        self.warning_var.set("")

    def _update_preview(self) -> None:
        selected = self._get_selected_items()
        if len(selected) != 1:
            self._clear_preview()
            return
        item = selected[0]
        try:
            result = self.renderer.render_preview(item)
        except Exception as exc:
            self.warning_var.set(f"Önizleme oluşturulamadı: {exc}")
            self._clear_preview()
            return
        self.preview_photo = ImageTk.PhotoImage(result.image)
        self.preview_label.configure(image=self.preview_photo)
        self._show_warnings(result.warnings)

    def _show_warnings(self, warnings: List[str]) -> None:
        self.warning_var.set("\n".join(dict.fromkeys(warnings)))

    # endregion

    # region actions
    def on_save_pdf(self) -> None:
        selected = self._get_selected_items()
        if len(selected) != 1:
            return
        item = selected[0]
        file_path = filedialog.asksaveasfilename(
            parent=self.window,
            title="Etiketi kaydet",
            defaultextension=".pdf",
            filetypes=[("PDF", "*.pdf"), ("PNG", "*.png")],
        )
        if not file_path:
            return
        try:
            if file_path.lower().endswith(".png"):
                warnings = self.renderer.export_png(item, file_path)
            else:
                warnings = self.renderer.export_pdf([item], file_path)
        except Exception as exc:
            messagebox.showerror("Kaydet", f"Dosya kaydedilirken hata oluştu: {exc}")
            return
        self._show_result_message("Kaydet", "Etiket kaydedildi.", warnings)

    def on_save_bulk_pdf(self) -> None:
        selected = self._get_selected_items()
        if len(selected) <= 1:
            return
        file_path = filedialog.asksaveasfilename(
            parent=self.window,
            title="Toplu PDF kaydet",
            defaultextension=".pdf",
            filetypes=[("PDF", "*.pdf")],
        )
        if not file_path:
            return
        try:
            warnings = self.renderer.export_pdf(selected, file_path)
        except Exception as exc:
            messagebox.showerror("Toplu PDF", f"PDF oluşturulamadı: {exc}")
            return
        self._show_result_message("Toplu PDF", "PDF kaydedildi.", warnings)

    def on_print(self) -> None:
        selected = self._get_selected_items()
        if len(selected) != 1:
            return
        item = selected[0]
        try:
            warnings = self.renderer.print_to_default(item)
        except Exception as exc:
            messagebox.showerror("Yazdır", f"Yazdırma başlatılamadı: {exc}")
            return
        self._show_result_message("Yazdır", "Yazdırma işlemi başlatıldı.", warnings)

    def _show_result_message(self, title: str, message: str, warnings: List[str]) -> None:
        self._show_warnings(warnings)
        if warnings:
            messagebox.showwarning(title, f"{message}\n\nUyarılar:\n- " + "\n- ".join(warnings))
        else:
            messagebox.showinfo(title, message)

    def close(self) -> None:
        if self.window.winfo_exists():
            try:
                self.window.grab_release()
            except tk.TclError:
                pass
            self.window.destroy()
        if self._on_close:
            self._on_close()

    # endregion


__all__ = ["LabelGeneratorWindow"]
