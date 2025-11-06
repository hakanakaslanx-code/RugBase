"""Utility helpers for barcode scanner entry bindings."""
from __future__ import annotations

from typing import Callable

import tkinter as tk


class BarcodeScanner:
    """Bind an Entry widget to invoke a callback when Return is pressed."""

    def __init__(self, entry: tk.Entry, callback: Callable[[str], None]) -> None:
        self.entry = entry
        self.callback = callback
        self.entry.bind("<Return>", self._on_return)
        self.entry.bind("<KP_Enter>", self._on_return)
        self.entry.focus_set()

    def _on_return(self, _event: tk.Event) -> None:  # type: ignore[name-defined]
        data = self.entry.get().strip()
        if data:
            self.callback(data)
        self.entry.delete(0, tk.END)


def bind_scanner(entry: tk.Entry, callback: Callable[[str], None]) -> BarcodeScanner:
    """Factory helper for attaching the scanner binding."""

    return BarcodeScanner(entry, callback)
