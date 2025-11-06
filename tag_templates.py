from __future__ import annotations

import io
from typing import Dict, Iterable, List

from barcode import Code39
from barcode.writer import ImageWriter
from PIL import Image
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas as pdf_canvas


PAGE_WIDTH = 2.0 * inch
PAGE_HEIGHT = 4.0 * inch
PAGE_MARGIN = 0.15 * inch


def generate_dymo_2x4_vertical_pdf(items: Iterable[Dict], output_path: str) -> None:
    canvas = pdf_canvas.Canvas(output_path, pagesize=(PAGE_WIDTH, PAGE_HEIGHT))

    for item in items:
        _draw_dymo_vertical_page(canvas, item)
        canvas.showPage()

    canvas.save()


def _draw_dymo_vertical_page(canvas: pdf_canvas.Canvas, item: Dict) -> None:
    left_x = PAGE_MARGIN + 0.03 * inch
    line_spacing = 0.18 * inch
    current_y = PAGE_HEIGHT - PAGE_MARGIN - 0.20 * inch

    collection_text = _format_label_value("Collection", item.get("collection"))
    design_text = _format_label_value("Design", item.get("design"))
    color_text = _format_label_value("Color", _format_color(item))
    size_text = _format_label_value("Size", item.get("size_label"))

    canvas.setFont("Helvetica-Bold", 10)
    canvas.drawString(left_x, current_y, collection_text)
    current_y -= line_spacing

    canvas.setFont("Helvetica", 10)
    canvas.drawString(left_x, current_y, design_text)
    current_y -= line_spacing

    canvas.setFont("Helvetica", 9)
    canvas.drawString(left_x, current_y, color_text)
    current_y -= line_spacing

    canvas.drawString(left_x, current_y, size_text)

    _draw_center_rug_number(canvas, item)
    _draw_barcode(canvas, item)
    _draw_bottom_block(canvas, item)


def _draw_center_rug_number(canvas: pdf_canvas.Canvas, item: Dict) -> None:
    center_x = PAGE_WIDTH / 2
    label_y = 2.75 * inch
    value_y = 2.35 * inch

    rug_number = _get_rug_number(item)

    canvas.setFont("Helvetica", 8)
    canvas.drawCentredString(center_x, label_y, "Rug #")

    canvas.setFont("Helvetica-Bold", 18)
    canvas.drawCentredString(center_x, value_y, rug_number)


def _draw_barcode(canvas: pdf_canvas.Canvas, item: Dict) -> None:
    barcode_width = 1.1 * inch
    barcode_height = 0.60 * inch
    top_left_x = 0.85 * inch
    top_left_y = 2.6 * inch

    payload = _get_barcode_payload(item)
    barcode_image = _create_code39_image(payload)
    image_reader = ImageReader(barcode_image)

    img_width, img_height = barcode_image.size
    aspect_ratio = img_width / img_height if img_height else 1.0

    target_width = barcode_width
    target_height = target_width / aspect_ratio
    if target_height > barcode_height:
        target_height = barcode_height
        target_width = target_height * aspect_ratio

    draw_x = top_left_x
    draw_y = top_left_y - target_height

    canvas.drawImage(image_reader, draw_x, draw_y, width=target_width, height=target_height, preserveAspectRatio=True)

    human_text = f"*{payload}*"
    canvas.setFont("Helvetica", 8)
    canvas.drawCentredString(draw_x + (target_width / 2), draw_y - 0.12 * inch, human_text)


def _draw_bottom_block(canvas: pdf_canvas.Canvas, item: Dict) -> None:
    left_x = PAGE_MARGIN + 0.03 * inch
    start_y = 0.95 * inch
    line_spacing = 0.16 * inch

    origin_content = _format_origin_content(item.get("notes"))
    price_text = _format_price(item.get("price_list"))
    sku_text = _format_sku(item.get("sku"))

    canvas.setFont("Helvetica", 8)
    if origin_content:
        canvas.drawString(left_x, start_y, origin_content)
        start_y -= line_spacing

    if price_text:
        canvas.setFont("Helvetica-Bold", 12)
        canvas.drawString(left_x, start_y, price_text)
        start_y -= line_spacing
        canvas.setFont("Helvetica", 8)

    if sku_text:
        canvas.drawString(left_x, start_y, sku_text)


def _get_rug_number(item: Dict) -> str:
    rug_no = (item.get("rug_no") or "").strip()
    if rug_no:
        return rug_no
    fallback = (item.get("item_id") or "").strip()
    return fallback or ""


def _get_barcode_payload(item: Dict) -> str:
    payload = _get_rug_number(item)
    if payload:
        return payload
    fallback = (item.get("item_id") or "").strip()
    return fallback or "UNKNOWN"


def _format_label_value(label: str, value: str | None) -> str:
    value = (value or "").strip()
    return f"{label}: {value}" if value else f"{label}:"


def _format_color(item: Dict) -> str:
    ground = (item.get("ground") or "").strip()
    border = (item.get("border") or "").strip()
    parts: List[str] = []
    if ground:
        parts.append(ground)
    if border:
        parts.append(border)
    return "/".join(parts)


def _format_origin_content(notes: str | None) -> str:
    if not notes:
        return ""

    origin = ""
    content = ""
    for line in notes.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("origin:"):
            origin = stripped.split(":", 1)[1].strip()
        elif stripped.lower().startswith("content:"):
            content = stripped.split(":", 1)[1].strip()

    parts = [part for part in (origin, content) if part]
    if not parts:
        return ""

    return f"Origin / Content: {' • '.join(parts)}"


def _format_price(price: float | None) -> str:
    if price is None:
        return ""
    try:
        return f"Price ${float(price):.2f}"
    except (TypeError, ValueError):
        return ""


def _format_sku(sku: str | None) -> str:
    sku = (sku or "").strip()
    return f"SKU {sku}" if sku else ""


def _create_code39_image(payload: str) -> Image.Image:
    code39 = Code39(payload, writer=ImageWriter(), add_checksum=False)
    buffer = io.BytesIO()
    writer_options = {
        "module_width": 0.2,
        "module_height": 12.0,
        "quiet_zone": 1.0,
        "font_size": 0,
        "text_distance": 0,
        "write_text": False,
    }
    code39.write(buffer, options=writer_options)
    buffer.seek(0)
    image = Image.open(buffer)
    image.load()
    buffer.close()
    return image
