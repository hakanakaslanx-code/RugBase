"""Tkinter UI components for consignment workflows."""
from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

import consignment_repo
from scanner import bind_scanner


@dataclass
class ScanResult:
    rug_no: str
    message: str


class ConsignmentModal:
    """Modal window for scanning rugs out to consignments."""

    def __init__(self, master: tk.Tk, user: str) -> None:
        self.master = master
        self.user = user
        self.window = tk.Toplevel(master)
        self.window.title("Consignment Scan Out")
        self.window.grab_set()
        self.window.transient(master)
        self.window.geometry("600x500")
        self.window.resizable(False, False)

        self._consignments: List[Dict[str, Any]] = []
        self._existing_map: Dict[str, int] = {}
        self.active_consignment: Optional[Dict[str, Any]] = None

        self.mode_var = tk.StringVar(value="existing")
        self.partner_var = tk.StringVar()
        self.contact_var = tk.StringVar()
        self.notes_var = tk.StringVar()
        self.existing_var = tk.StringVar()

        self.scan_results: List[ScanResult] = []

        self._build_ui()
        self.refresh_consignments()
        self.refresh_partner_names()

    def _build_ui(self) -> None:
        container = ttk.Frame(self.window, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        header = ttk.Label(container, text="Consignment Management", font=("Arial", 14, "bold"))
        header.pack(anchor=tk.W, pady=(0, 10))

        mode_frame = ttk.LabelFrame(container, text="Consignment Selection")
        mode_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Radiobutton(
            mode_frame,
            text="Existing Consignment",
            variable=self.mode_var,
            value="existing",
            command=self._on_mode_change,
        ).grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)

        ttk.Radiobutton(
            mode_frame,
            text="New Consignment",
            variable=self.mode_var,
            value="new",
            command=self._on_mode_change,
        ).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)

        ttk.Label(mode_frame, text="Existing:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.E)
        self.existing_combo = ttk.Combobox(mode_frame, textvariable=self.existing_var, state="readonly", width=40)
        self.existing_combo.grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(mode_frame, text="Partner:").grid(row=2, column=0, padx=5, pady=5, sticky=tk.E)
        self.partner_entry = ttk.Combobox(
            mode_frame,
            textvariable=self.partner_var,
            width=38,
            state="normal",
        )
        self.partner_entry.grid(row=2, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(mode_frame, text="Contact:").grid(row=3, column=0, padx=5, pady=5, sticky=tk.E)
        self.contact_entry = ttk.Entry(mode_frame, textvariable=self.contact_var, width=40)
        self.contact_entry.grid(row=3, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(mode_frame, text="Notes:").grid(row=4, column=0, padx=5, pady=5, sticky=tk.E)
        self.notes_entry = ttk.Entry(mode_frame, textvariable=self.notes_var, width=40)
        self.notes_entry.grid(row=4, column=1, padx=5, pady=5, sticky=tk.W)

        mode_frame.columnconfigure(1, weight=1)

        scan_frame = ttk.LabelFrame(container, text="Scan Mode")
        scan_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        ttk.Label(scan_frame, text="Barcode / Rug #:").pack(anchor=tk.W, padx=5, pady=(5, 0))
        self.scan_entry = ttk.Entry(scan_frame)
        self.scan_entry.pack(fill=tk.X, padx=5, pady=5)
        bind_scanner(self.scan_entry, self._handle_scan)

        self.status_var = tk.StringVar()
        ttk.Label(scan_frame, textvariable=self.status_var, foreground="blue").pack(anchor=tk.W, padx=5, pady=(0, 5))

        columns = ("rug_no", "message")
        self.result_tree = ttk.Treeview(scan_frame, columns=columns, show="headings", height=10)
        self.result_tree.heading("rug_no", text="Rug No")
        self.result_tree.heading("message", text="Status")
        self.result_tree.column("rug_no", width=140)
        self.result_tree.column("message", width=360)
        self.result_tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        button_frame = ttk.Frame(container)
        button_frame.pack(fill=tk.X)

        ttk.Button(button_frame, text="Finish / Close", command=self.window.destroy).pack(side=tk.RIGHT)

        self._on_mode_change()

    def refresh_consignments(self) -> None:
        self._consignments = consignment_repo.fetch_active_consignments()
        self._existing_map.clear()
        display_values: List[str] = []
        for consignment in self._consignments:
            display = f"{consignment['consignment_ref']} - {consignment['partner_name']}"
            display_values.append(display)
            self._existing_map[display] = consignment["id"]
        self.existing_combo.configure(values=display_values)
        if display_values:
            self.existing_combo.current(0)
            self.existing_var.set(display_values[0])
        else:
            self.existing_var.set("")

    def refresh_partner_names(self) -> None:
        partners = consignment_repo.fetch_partner_names()
        self.partner_entry.configure(values=partners)

    def _on_mode_change(self) -> None:
        is_new = self.mode_var.get() == "new"
        if is_new:
            self.partner_entry.configure(state="normal")
        else:
            self.partner_entry.configure(state="disabled")
        state = tk.NORMAL if is_new else tk.DISABLED
        for widget in (self.contact_entry, self.notes_entry):
            widget.configure(state=state)
        if not is_new:
            self.active_consignment = None
            self.status_var.set("Existing consignment selected.")
        else:
            self.status_var.set("Enter partner details to create a new consignment.")

    def _append_result(self, rug_no: str, message: str) -> None:
        result = ScanResult(rug_no=rug_no, message=message)
        self.scan_results.append(result)
        self.result_tree.insert("", tk.END, values=(rug_no, message))

    def _handle_scan(self, rug_no: str) -> None:
        try:
            if self.mode_var.get() == "new":
                if not self.active_consignment:
                    partner = self.partner_var.get().strip()
                    if not partner:
                        raise ValueError("Partner name is required")
                    new_data = {
                        "partner_name": partner,
                        "partner_contact": self.contact_var.get().strip() or None,
                        "notes": self.notes_var.get().strip() or None,
                    }
                    item, consignment = consignment_repo.process_scan(
                        rug_no,
                        self.user,
                        new_consignment_data=new_data,
                    )
                    self.active_consignment = consignment
                    self.refresh_consignments()
                    self.refresh_partner_names()
                    display = f"{consignment['consignment_ref']} - {consignment['partner_name']}"
                    self.existing_var.set(display)
                    self.status_var.set(
                        f"Created consignment: {consignment['consignment_ref']}"
                    )
                else:
                    item, consignment = consignment_repo.process_scan(
                        rug_no,
                        self.user,
                        consignment_id=self.active_consignment["id"],
                    )
            else:
                selected = self.existing_var.get()
                consignment_id = self._existing_map.get(selected)
                if not consignment_id:
                    raise ValueError("Please choose an existing consignment")
                item, consignment = consignment_repo.process_scan(
                    rug_no,
                    self.user,
                    consignment_id=consignment_id,
                )
            message = f"Checked out → {consignment['consignment_ref']}"
            self._append_result(rug_no, message)
            self.status_var.set(message)
        except Exception as exc:
            messagebox.showerror("Consignment", str(exc), parent=self.window)
            self._append_result(rug_no, f"Error: {exc}")
            self.status_var.set(f"Error: {exc}")


class ReturnModal:
    """Modal window for processing consignment returns."""

    def __init__(self, master: tk.Tk, user: str) -> None:
        self.master = master
        self.user = user
        self.window = tk.Toplevel(master)
        self.window.title("Consignment Return")
        self.window.grab_set()
        self.window.transient(master)
        self.window.geometry("500x450")
        self.window.resizable(False, False)

        self.scan_results: List[ScanResult] = []

        self._build_ui()

    def _build_ui(self) -> None:
        container = ttk.Frame(self.window, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        header = ttk.Label(container, text="Consignment Return Scanning", font=("Arial", 14, "bold"))
        header.pack(anchor=tk.W, pady=(0, 10))

        scan_frame = ttk.LabelFrame(container, text="Scan Mode")
        scan_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        ttk.Label(scan_frame, text="Barcode / Rug #:").pack(anchor=tk.W, padx=5, pady=(5, 0))
        self.scan_entry = ttk.Entry(scan_frame)
        self.scan_entry.pack(fill=tk.X, padx=5, pady=5)
        bind_scanner(self.scan_entry, self._handle_scan)

        self.status_label = ttk.Label(scan_frame, foreground="blue")
        self.status_label.pack(anchor=tk.W, padx=5, pady=(0, 5))

        columns = ("rug_no", "message")
        self.result_tree = ttk.Treeview(scan_frame, columns=columns, show="headings", height=12)
        self.result_tree.heading("rug_no", text="Rug No")
        self.result_tree.heading("message", text="Status")
        self.result_tree.column("rug_no", width=150)
        self.result_tree.column("message", width=320)
        self.result_tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        ttk.Button(container, text="Finish / Close", command=self.window.destroy).pack(
            anchor=tk.E
        )

    def _append_result(self, rug_no: str, message: str) -> None:
        result = ScanResult(rug_no=rug_no, message=message)
        self.scan_results.append(result)
        self.result_tree.insert("", tk.END, values=(rug_no, message))

    def _handle_scan(self, rug_no: str) -> None:
        try:
            item, consignment = consignment_repo.process_return_scan(rug_no, self.user)
            self._append_result(
                rug_no,
                f"Returned ← {consignment['consignment_ref']}",
            )
            self.status_label.configure(
                text=f"Last action: {rug_no} → {consignment['consignment_ref']}",
                foreground="blue",
            )
        except Exception as exc:
            messagebox.showerror("Return", str(exc), parent=self.window)
            self._append_result(rug_no, f"Error: {exc}")
            self.status_label.configure(text=f"Error: {exc}", foreground="red")


class ConsignmentDetailWindow:
    """Detail window showing items for a given consignment."""

    def __init__(self, master: tk.Tk, consignment: Dict[str, str]) -> None:
        self.master = master
        self.consignment = consignment
        self.window = tk.Toplevel(master)
        self.window.title(f"Consignment Detail - {consignment['consignment_ref']}")
        self.window.geometry("700x500")
        self.window.transient(master)
        self.window.grab_set()

        self._build_ui()

    def _build_ui(self) -> None:
        container = ttk.Frame(self.window, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        info = (
            f"Ref: {self.consignment['consignment_ref']}\n"
            f"Partner: {self.consignment['partner_name']}\n"
            f"Status: {self.consignment['status']}"
        )
        ttk.Label(container, text=info, justify=tk.LEFT).pack(anchor=tk.W, pady=(0, 10))

        columns = ("rug_no", "state", "scanned_at", "collection", "design", "brand_name")
        self.tree = ttk.Treeview(container, columns=columns, show="headings", height=15)
        headers = {
            "rug_no": "Rug No",
            "state": "State",
            "scanned_at": "Date",
            "collection": "Collection",
            "design": "Design",
            "brand_name": "Brand",
        }
        for key, label in headers.items():
            self.tree.heading(key, text=label)
            self.tree.column(key, width=110 if key != "scanned_at" else 150)
        self.tree.pack(fill=tk.BOTH, expand=True)

        button_frame = ttk.Frame(container)
        button_frame.pack(fill=tk.X, pady=(10, 0))

        ttk.Button(button_frame, text="Export CSV", command=self._export_csv).pack(side=tk.LEFT)
        ttk.Button(button_frame, text="Close", command=self.window.destroy).pack(side=tk.RIGHT)

        self._load_lines()

    def _load_lines(self) -> None:
        for row in self.tree.get_children():
            self.tree.delete(row)
        lines = consignment_repo.fetch_consignment_lines(self.consignment["id"])
        for line in lines:
            self.tree.insert(
                "",
                tk.END,
                values=(
                    line.get("rug_no"),
                    line.get("state"),
                    line.get("scanned_at"),
                    line.get("collection") or "",
                    line.get("design") or "",
                    line.get("brand_name") or "",
                ),
            )

    def _export_csv(self) -> None:
        default_name = f"{self.consignment['consignment_ref']}_lines.csv"
        filename = filedialog.asksaveasfilename(
            parent=self.window,
            defaultextension=".csv",
            initialfile=default_name,
            filetypes=[("CSV", "*.csv")],
        )
        if not filename:
            return

        lines = consignment_repo.fetch_consignment_lines(self.consignment["id"])
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                "rug_no",
                "state",
                "scanned_at",
                "collection",
                "design",
                "brand_name",
            ])
            for line in lines:
                writer.writerow(
                    [
                        line.get("rug_no"),
                        line.get("state"),
                        line.get("scanned_at"),
                        line.get("collection") or "",
                        line.get("design") or "",
                        line.get("brand_name") or "",
                    ]
                )
        messagebox.showinfo(
            "CSV Export",
            f"File saved: {os.path.basename(filename)}",
            parent=self.window,
        )


class ConsignmentListWindow:
    """Window that shows all consignments with ability to export CSV."""

    def __init__(self, master: tk.Tk) -> None:
        self.master = master
        self.window = tk.Toplevel(master)
        self.window.title("Consignment List")
        self.window.geometry("750x500")
        self.window.transient(master)
        self.window.grab_set()
        self._consignments: List[Dict[str, Any]] = []

        self._build_ui()
        self._load_consignments()

    def _build_ui(self) -> None:
        container = ttk.Frame(self.window, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        columns = (
            "consignment_ref",
            "partner_name",
            "created_at",
            "status",
            "total_out",
            "total_returned",
        )
        self.tree = ttk.Treeview(container, columns=columns, show="headings", height=18)
        headers = {
            "consignment_ref": "Ref",
            "partner_name": "Partner",
            "created_at": "Date",
            "status": "Status",
            "total_out": "Checked Out",
            "total_returned": "Returned",
        }
        widths = {
            "consignment_ref": 120,
            "partner_name": 160,
            "created_at": 140,
            "status": 100,
            "total_out": 80,
            "total_returned": 90,
        }
        for key, label in headers.items():
            self.tree.heading(key, text=label)
            self.tree.column(key, width=widths[key])
        self.tree.pack(fill=tk.BOTH, expand=True)
        self.tree.bind("<Double-1>", self._on_double_click)

        button_frame = ttk.Frame(container)
        button_frame.pack(fill=tk.X, pady=(10, 0))

        ttk.Button(button_frame, text="Export CSV", command=self._export_csv).pack(side=tk.LEFT)
        ttk.Button(button_frame, text="Close", command=self.window.destroy).pack(side=tk.RIGHT)

    def _load_consignments(self) -> None:
        for row in self.tree.get_children():
            self.tree.delete(row)
        self._consignments = consignment_repo.fetch_all_consignments()
        for consignment in self._consignments:
            self.tree.insert(
                "",
                tk.END,
                iid=str(consignment["id"]),
                values=(
                    consignment.get("consignment_ref"),
                    consignment.get("partner_name"),
                    consignment.get("created_at"),
                    consignment.get("status"),
                    consignment.get("total_out"),
                    consignment.get("total_returned"),
                ),
            )

    def _export_csv(self) -> None:
        filename = filedialog.asksaveasfilename(
            parent=self.window,
            defaultextension=".csv",
            initialfile="consignments.csv",
            filetypes=[("CSV", "*.csv")],
        )
        if not filename:
            return

        consignments = consignment_repo.fetch_all_consignments()
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                "consignment_ref",
                "partner_name",
                "partner_contact",
                "created_at",
                "status",
                "notes",
                "total_out",
                "total_returned",
            ])
            for consignment in consignments:
                writer.writerow(
                    [
                        consignment.get("consignment_ref"),
                        consignment.get("partner_name"),
                        consignment.get("partner_contact"),
                        consignment.get("created_at"),
                        consignment.get("status"),
                        consignment.get("notes"),
                        consignment.get("total_out"),
                        consignment.get("total_returned"),
                    ]
                )
        messagebox.showinfo(
            "CSV Export",
            f"File saved: {os.path.basename(filename)}",
            parent=self.window,
        )

    def _on_double_click(self, _event: tk.Event) -> None:  # type: ignore[name-defined]
        selection = self.tree.selection()
        if not selection:
            return
        consignment_id = int(selection[0])
        consignment = next(
            (c for c in self._consignments if c["id"] == consignment_id),
            None,
        )
        if consignment:
            ConsignmentDetailWindow(self.window, consignment)
