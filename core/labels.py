"""Utilities for generating PDF labels for rugs."""

from __future__ import annotations

import os
import tempfile
import webbrowser
from pathlib import Path
from typing import Iterable, Mapping

from barcode import Code39
from barcode.writer import ImageWriter
from reportlab.lib.pagesizes import portrait
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

PAGE_WIDTH = 2.0 * inch
PAGE_HEIGHT = 4.0 * inch


def _draw_label(canv: canvas.Canvas, item: Mapping[str, object]) -> None:
    canv.setPageSize(portrait((PAGE_WIDTH, PAGE_HEIGHT)))

    margin_x = 0.15 * inch
    margin_y = 0.15 * inch
    text_x = margin_x
    current_y = PAGE_HEIGHT - margin_y

    collection = (item.get("collection") or "").strip()
    design = (item.get("design") or "").strip()
    ground = (item.get("ground") or "").strip()
    border = (item.get("border") or "").strip()
    size_label = (item.get("size_label") or "").strip()
    rug_no = (item.get("rug_no") or "").strip()
    price_list = item.get("price_list")
    sku = (item.get("sku") or "").strip()

    canv.setFont("Helvetica-Bold", 10)
    canv.drawString(text_x, current_y, collection)
    current_y -= 12

    canv.setFont("Helvetica", 10)
    canv.drawString(text_x, current_y, design)
    current_y -= 12

    canv.setFont("Helvetica", 9)
    if ground and border:
        color_text = f"{ground}/{border}"
    else:
        color_text = ground or border
    canv.drawString(text_x, current_y, color_text)
    current_y -= 11

    canv.drawString(text_x, current_y, size_label)
    current_y -= 0.12 * inch

    canv.setFont("Helvetica", 7)
    canv.drawCentredString(PAGE_WIDTH / 2.0, current_y, "RUG #")
    current_y -= 10

    canv.setFont("Helvetica-Bold", 18)
    canv.drawCentredString(PAGE_WIDTH / 2.0, current_y, rug_no)

    barcode_height = 0.6 * inch
    barcode_width = 0.9 * inch
    barcode_x = PAGE_WIDTH - margin_x - barcode_width
    barcode_y = PAGE_HEIGHT / 2.0 - barcode_height / 2.0

    if rug_no:
        with tempfile.TemporaryDirectory() as tmpdir:
            barcode = Code39(rug_no, writer=ImageWriter(), add_checksum=False)
            barcode_filename = barcode.save(
                os.path.join(tmpdir, "barcode"),
                options={
                    "module_height": barcode_height / inch * 25.4,
                    "write_text": False,
                },
            )
            canv.drawImage(
                barcode_filename,
                barcode_x,
                barcode_y,
                width=barcode_width,
                height=barcode_height,
                preserveAspectRatio=True,
            )
    if rug_no:
        text_baseline = barcode_y - 10
        canv.setFont("Helvetica", 8)
        canv.drawCentredString(barcode_x + barcode_width / 2.0, text_baseline, "*RUGNO*")

    footer_y = margin_y + 20
    if price_list not in (None, ""):
        canv.setFont("Helvetica-Bold", 12)
        canv.drawString(text_x, footer_y, f"Price ${price_list}")
        footer_y -= 14

    if sku:
        canv.setFont("Helvetica", 8)
        canv.drawString(text_x, footer_y, f"SKU {sku}")


def create_tag_labels_pdf(items: Iterable[Mapping[str, object]], output_path: str | os.PathLike[str] | None = None) -> Path:
    """Create a multi-page PDF containing rug labels and return the file path."""

    items = list(items)
    if not items:
        raise ValueError("No items provided for label generation.")

    if output_path is None:
        fd, temp_name = tempfile.mkstemp(suffix=".pdf", prefix="rug_labels_")
        os.close(fd)
        output_path = Path(temp_name)
    else:
        output_path = Path(output_path)

    canv = canvas.Canvas(str(output_path), pagesize=portrait((PAGE_WIDTH, PAGE_HEIGHT)))

    for item in items:
        _draw_label(canv, item)
        canv.showPage()

    canv.save()
    return output_path


def open_pdf(path: str | os.PathLike[str]) -> None:
    pdf_path = Path(path)
    if os.name == "nt":
        os.startfile(str(pdf_path))  # type: ignore[attr-defined]
    else:
        webbrowser.open(pdf_path.as_uri())
