"""
Utility helpers for rendering Rinven barcode labels.

This module focuses on three main tasks:
- Rendering barcodes for Rinven labels.
- Creating barcode images with adjustable module sizes.
- Building tag images that combine barcode graphics and textual data.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence, Tuple

from PIL import Image, ImageDraw, ImageFont

DEFAULT_DPI = 300


@dataclass
class BarcodeRenderResult:
    """Container for barcode render outcomes."""

    value: str
    image: Image.Image


# -----------------------------
# Barcode rendering helpers
# -----------------------------


def _render_barcode_image(
    value: str,
    *,
    module_height: float = 18.0,
    module_width: float = 1.2,
    quiet_zone: int = 6,
    bar_color: str = "black",
    background: str = "white",
) -> BarcodeRenderResult:
    """Render a lightweight, text-driven barcode.

    This uses a predictable pattern derived from the input string to avoid
    bringing in heavyweight dependencies. The effective writer options mimic
    python-barcode's API so other callers can reason about sizing changes.
    """

    effective_writer_options = {
        "module_height": module_height,
        "module_width": module_width,
        "quiet_zone": quiet_zone,
        "background": background,
        "foreground": bar_color,
    }

    # Build a deterministic bit pattern from the string content.
    bits: List[int] = []
    for char in value.encode("utf-8"):
        bits.extend(int(bit) for bit in f"{char:08b}")
    # Guarantee a start/stop guard
    pattern = [1, 0, 1, 0, 1, 0] + bits + [0, 1, 0, 1, 0, 1]

    bar_height_px = int(round(module_height))
    bar_width_px = max(int(round(module_width)), 1)

    width_px = quiet_zone * 2 + bar_width_px * len(pattern)
    height_px = max(bar_height_px, 1)

    image = Image.new("RGB", (width_px, height_px), background)
    draw = ImageDraw.Draw(image)

    cursor = quiet_zone
    for is_bar in pattern:
        if is_bar:
            draw.rectangle(
                [cursor, 0, cursor + bar_width_px - 1, height_px - 1],
                fill=bar_color,
            )
        cursor += bar_width_px

    return BarcodeRenderResult(value=value, image=image)


def _render_rinven_barcode(value: str) -> BarcodeRenderResult:
    """Render a Rinven-specific barcode using the tuned module height."""

    return _render_barcode_image(value, module_height=18.0)


# -----------------------------
# Tasks
# -----------------------------


def generate_barcode_task(value: str, output_path: Path) -> Path:
    """Generate a barcode PNG at the given path.

    The barcode writer previously used an excessively large module height
    derived from ``int(0.7 * 300)``. The tuned 18.0 height matches the
    desired proportions for our labels.
    """

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    result = _render_barcode_image(value, module_height=18.0)
    result.image.save(output_path, format="PNG")
    return output_path


# -----------------------------
# Tag assembly helpers
# -----------------------------


def _load_font(size: int) -> ImageFont.ImageFont:
    try:
        return ImageFont.truetype("arial.ttf", size)
    except OSError:
        return ImageFont.load_default()


def build_rinven_tag_image(
    text_lines: Sequence[str],
    barcode_value: str,
    *,
    dpi: int = DEFAULT_DPI,
    padding: int = 10,
    tag_size: Tuple[int, int] = (400, 250),
) -> Image.Image:
    """Build a tag image with centered text and a barcode footer."""

    tag_width, tag_height = tag_size
    image = Image.new("RGB", (tag_width, tag_height), "white")
    draw = ImageDraw.Draw(image)

    text_font_size = int(dpi * 0.16)
    min_text_size = int(dpi * 0.08)
    primary_font = _load_font(text_font_size)
    secondary_font = _load_font(min_text_size)

    # Measure the text block to compute a centered starting x coordinate.
    line_metrics: List[Tuple[int, int]] = []
    for line in text_lines:
        bbox = draw.textbbox((0, 0), line, font=primary_font)
        width = bbox[2] - bbox[0]
        height = bbox[3] - bbox[1]
        line_metrics.append((width, height))
    block_width = max((w for w, _ in line_metrics), default=0)
    start_x = max(int((tag_width - block_width) / 2), padding)

    current_y = padding
    for (width, height), line in zip(line_metrics, text_lines):
        draw.text((start_x, current_y), line, fill="black", font=primary_font)
        current_y += height + 4

    # Render the barcode near the bottom of the tag.
    barcode_result = _render_rinven_barcode(barcode_value)
    barcode_img = barcode_result.image
    barcode_x = max(int((tag_width - barcode_img.width) / 2), padding)
    barcode_y = min(current_y + 8, tag_height - barcode_img.height - padding)
    image.paste(barcode_img, (barcode_x, barcode_y))

    # Add the barcode value beneath the graphic using the smaller font.
    text_bbox = draw.textbbox((0, 0), barcode_value, font=secondary_font)
    barcode_text_width = text_bbox[2] - text_bbox[0]
    barcode_text_height = text_bbox[3] - text_bbox[1]
    text_x = max(int((tag_width - barcode_text_width) / 2), padding)
    text_y = barcode_y + barcode_img.height + 2
    draw.text((text_x, text_y), barcode_value, fill="black", font=secondary_font)

    return image


__all__ = [
    "BarcodeRenderResult",
    "build_rinven_tag_image",
    "generate_barcode_task",
    "_render_barcode_image",
    "_render_rinven_barcode",
]
