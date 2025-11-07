from __future__ import annotations

import math
import os
import re
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from core import dependencies

Image = None  # type: ignore[assignment]
ImageDraw = None  # type: ignore[assignment]
ImageFont = None  # type: ignore[assignment]
PIL_IMPORT_MESSAGE = (
    "Pillow (PIL) is required to generate labels. The application will try to install it "
    "automatically, but manual installation via 'pip install Pillow' may be necessary."
)


def _import_pillow() -> bool:
    """Attempt to import Pillow and cache the modules on success."""

    global Image, ImageDraw, ImageFont
    try:
        from PIL import Image as _Image, ImageDraw as _ImageDraw, ImageFont as _ImageFont  # type: ignore
    except ModuleNotFoundError:
        return False
    else:  # pragma: no cover - exercised when Pillow is installed
        Image = _Image
        ImageDraw = _ImageDraw
        ImageFont = _ImageFont
        return True


PIL_AVAILABLE = _import_pillow()
_PIL_INSTALL_ATTEMPTED = False


def ensure_pillow() -> bool:
    """Ensure Pillow is installed and imported, attempting automatic installation."""

    global PIL_AVAILABLE, PIL_IMPORT_MESSAGE, _PIL_INSTALL_ATTEMPTED
    if PIL_AVAILABLE:
        return True

    if not _PIL_INSTALL_ATTEMPTED:
        _PIL_INSTALL_ATTEMPTED = True
        success, message = dependencies.install_packages(["Pillow"])
        if not success:
            PIL_IMPORT_MESSAGE = (
                "Pillow (PIL) is required to generate labels and could not be installed automatically. "
                f"Please install it manually. Details: {message}"
            )
            return False
        if _import_pillow():
            PIL_AVAILABLE = True
            PIL_IMPORT_MESSAGE = ""
            return True
        PIL_IMPORT_MESSAGE = (
            "Pillow (PIL) was installed but could not be imported. Please restart the application "
            "or install Pillow manually."
        )
        return False

    # Installation already attempted but failed; message is populated.
    return False

import db
from settings import DymoLabelSettings, FontSpec, load_settings

INCH_TO_MM = 25.4
POINTS_PER_INCH = 72


@dataclass
class RenderResult:
    image: Any
    warnings: List[str]


class Barcode39:
    _PATTERNS: Dict[str, str] = {
        "0": "nnnwwnwnn",
        "1": "wnnwnnnnw",
        "2": "nnwwnnnnw",
        "3": "wnwwnnnnn",
        "4": "nnnwwnnnw",
        "5": "wnnwwnnnn",
        "6": "nnwwwnnnn",
        "7": "nnnwnnwnw",
        "8": "wnnwnnwnn",
        "9": "nnwwnnwnn",
        "A": "wnnnnwnnw",
        "B": "nnwnnwnnw",
        "C": "wnwnnwnnn",
        "D": "nnnnwwnnw",
        "E": "wnnnwwnnn",
        "F": "nnwnwwnnn",
        "G": "nnnnnwwnw",
        "H": "wnnnnwwnn",
        "I": "nnwnnwwnn",
        "J": "nnnnwwwnn",
        "K": "wnnnnnnww",
        "L": "nnwnnnnww",
        "M": "wnwnnnnwn",
        "N": "nnnnwnnww",
        "O": "wnnnwnnwn",
        "P": "nnwnwnnwn",
        "Q": "nnnnnnwww",
        "R": "wnnnnnwwn",
        "S": "nnwnnnwwn",
        "T": "nnnnwnwwn",
        "U": "wwnnnnnnw",
        "V": "nwwnnnnnw",
        "W": "wwwnnnnnn",
        "X": "nwnnwnnnw",
        "Y": "wwnnwnnnn",
        "Z": "nwwnwnnnn",
        "-": "nwnnnnwnw",
        ".": "wwnnnnwnn",
        " ": "nwwnnnwnn",
        "$": "nwnwnwnnn",
        "/": "nwnwnnnwn",
        "+": "nwnnnwnwn",
        "%": "nnnwnwnwn",
        "*": "nwnnwnwnn",
    }

    def __init__(self, narrow_bar_px: int, wide_bar_px: int) -> None:
        self.narrow_bar_px = max(1, narrow_bar_px)
        self.wide_bar_px = max(self.narrow_bar_px * 2, wide_bar_px)

    def encode(self, data: str) -> List[int]:
        encoded_widths: List[int] = []
        for index, char in enumerate(data):
            pattern = self._PATTERNS.get(char)
            if not pattern:
                raise ValueError(f"Unsupported Code39 character: {char}")
            for pos, symbol in enumerate(pattern):
                width = self.wide_bar_px if symbol == "w" else self.narrow_bar_px
                encoded_widths.append(width if pos % 2 == 0 else -width)
            if index < len(data) - 1:
                encoded_widths.append(-self.narrow_bar_px)
        return encoded_widths

    @staticmethod
    def measure(widths: Sequence[int]) -> int:
        return sum(abs(value) for value in widths)

    def draw(
        self,
        draw: ImageDraw.ImageDraw,
        top_left: Tuple[int, int],
        height: int,
        widths: Sequence[int],
    ) -> Tuple[int, int]:
        x, y = top_left
        for width in widths:
            if width > 0:
                draw.rectangle([x, y, x + width - 1, y + height], fill=0)
            x += abs(width)
        return top_left[0], x


class DymoLabelRenderer:
    def __init__(self) -> None:
        ensure_pillow()
        self.settings: DymoLabelSettings = load_settings()
        self._font_cache: Dict[Tuple[str, int], Any] = {}
        self._pdf_dimensions: Optional[Tuple[float, float]] = None

    @property
    def pillow_available(self) -> bool:
        """Return ``True`` when Pillow is installed."""

        return PIL_AVAILABLE

    # region settings helpers
    def _mm_to_px(self, value_mm: float) -> int:
        px = value_mm / INCH_TO_MM * self.settings.dpi
        return max(1, int(round(px)))

    def _pt_to_px(self, value_pt: float) -> int:
        px = value_pt / POINTS_PER_INCH * self.settings.dpi
        return max(1, int(round(px)))

    def _load_font(self, spec: FontSpec) -> Tuple[Any, Optional[str]]:
        if not PIL_AVAILABLE:
            raise RuntimeError(PIL_IMPORT_MESSAGE)
        key = (spec.name, spec.size_pt)
        cached = self._font_cache.get(key)
        if cached:
            return cached, None
        try:
            font = ImageFont.truetype(spec.name, self._pt_to_px(spec.size_pt))
        except OSError:
            font = ImageFont.load_default()
            warning = (
                f"TrueType font '{spec.name}' could not be found. Using the default Pillow font."
            )
            self._font_cache[key] = font
            return font, warning
        else:
            self._font_cache[key] = font
            return font, None

    def _load_pdf_dimensions(self) -> Optional[Tuple[float, float]]:
        if self._pdf_dimensions is not None:
            return self._pdf_dimensions
        reference = self.settings.pdf_reference
        if not reference:
            return None
        path = db.resource_path(reference)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "rb") as handle:
                data = handle.read()
        except OSError:
            return None
        match = re.search(rb"/MediaBox\s*\[\s*([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+)\s*\]", data)
        if not match:
            return None
        try:
            lower_x = float(match.group(1))
            lower_y = float(match.group(2))
            upper_x = float(match.group(3))
            upper_y = float(match.group(4))
        except ValueError:
            return None
        width_pt = abs(upper_x - lower_x)
        height_pt = abs(upper_y - lower_y)
        self._pdf_dimensions = (width_pt / POINTS_PER_INCH * INCH_TO_MM, height_pt / POINTS_PER_INCH * INCH_TO_MM)
        return self._pdf_dimensions

    def _canvas_size(self) -> Tuple[int, int]:
        pdf_dims = self._load_pdf_dimensions()
        width_mm = pdf_dims[0] if pdf_dims else self.settings.width_mm
        height_mm = pdf_dims[1] if pdf_dims else self.settings.height_mm
        width_px = self._mm_to_px(width_mm)
        height_px = self._mm_to_px(height_mm)
        return width_px, height_px

    # endregion

    def _compose_field_rows(self, item: Dict[str, object]) -> List[Tuple[str, str]]:
        ground = (item.get("ground") or "").strip()
        border = (item.get("border") or "").strip()
        if ground and border and ground.lower() != border.lower():
            color_value = f"{ground}/{border}"
        else:
            color_value = ground or border
        style = (item.get("style") or "").strip()
        content_value = (item.get("content") or "").strip()
        type_value = (item.get("type") or "").strip()
        if not content_value and type_value:
            lowered = type_value.lower()
            if "hand" in lowered:
                content_value = "hand made"
            elif "machine" in lowered:
                content_value = "machine made"
            else:
                content_value = type_value
        shape_value = (item.get("shape") or "").strip()
        rows: List[Tuple[str, str]] = []
        mapping = [
            ("Design", item.get("design")),
            ("Color", color_value),
            ("Size", item.get("st_size") or item.get("a_size")),
            ("Origin", item.get("origin")),
            ("Style", style),
            ("Content", content_value),
            ("Type", shape_value),
            ("Rug #", item.get("rug_no")),
        ]
        for label, value in mapping:
            text = (value or "").strip()
            if text:
                rows.append((label, text))
        return rows

    def _format_price_lines(self, item: Dict[str, object]) -> List[str]:
        lines: List[str] = []
        msrp = item.get("msrp") or item.get("MSRP")
        if msrp:
            lines.append(f"MSRP $ {str(msrp).strip()}")
        price = item.get("sp") or item.get("SP")
        if price:
            lines.append(f"Price $ {str(price).strip()}")
        return lines

    def _sku_value(self, item: Dict[str, object]) -> Optional[str]:
        for key in ("sku", "SKU", "upc", "UPC"):
            value = item.get(key)
            if value:
                return str(value).strip()
        return None

    def render(self, item: Dict[str, object]) -> RenderResult:
        if not PIL_AVAILABLE:
            raise RuntimeError(PIL_IMPORT_MESSAGE)

        width_px, height_px = self._canvas_size()
        image = Image.new("L", (width_px, height_px), color=255)
        draw = ImageDraw.Draw(image)
        warnings: List[str] = []

        margin_left = self._mm_to_px(self.settings.margins.left)
        margin_top = self._mm_to_px(self.settings.margins.top)
        margin_right = self._mm_to_px(self.settings.margins.right)
        margin_bottom = self._mm_to_px(self.settings.margins.bottom)

        content_width = width_px - margin_left - margin_right

        barcode_spec = self.settings.barcode
        layout_spec = self.settings.layout

        narrow_bar_px = self._mm_to_px(barcode_spec.narrow_bar_mm)
        wide_bar_px = self._mm_to_px(barcode_spec.wide_bar_mm)
        quiet_zone_px = self._mm_to_px(barcode_spec.quiet_zone_mm)
        barcode_height_px = self._mm_to_px(barcode_spec.height_mm)
        barcode = Barcode39(narrow_bar_px, wide_bar_px)

        rug_no = (item.get("rug_no") or "").strip().upper()
        if not rug_no:
            warnings.append("Rug # is empty. Barcode generation was skipped.")
        else:
            encoded = f"*{rug_no}*"
            widths = barcode.encode(encoded)
            pattern_width = Barcode39.measure(widths)
            available = max(content_width - (quiet_zone_px * 2), pattern_width)
            offset = max(0, (available - pattern_width) // 2)
            start_x = margin_left + quiet_zone_px + offset
            start_y = margin_top
            barcode.draw(draw, (start_x, start_y), barcode_height_px, widths)

        gap_after_barcode = barcode_height_px + self._mm_to_px(barcode_spec.text_gap_mm)
        current_y = margin_top + gap_after_barcode

        collection_spec = self.settings.fonts.get("collection")
        if collection_spec:
            collection_font, warn = self._load_font(collection_spec)
            if warn:
                warnings.append(warn)
        else:
            collection_font = ImageFont.load_default()
        collection_value = (item.get("collection") or "").strip()
        if collection_value:
            text = f"{collection_value.lower()} collection"
            text_width, text_height = collection_font.getsize(text)
            draw.text(
                (
                    margin_left + (content_width - text_width) / 2,
                    current_y,
                ),
                text,
                fill=0,
                font=collection_font,
            )
            current_y += text_height + self._mm_to_px(layout_spec.collection_gap_mm)

        field_rows = self._compose_field_rows(item)
        label_font_spec = self.settings.fonts.get("field_label")
        value_font_spec = self.settings.fonts.get("field_value")
        label_font, warn = self._load_font(label_font_spec) if label_font_spec else (ImageFont.load_default(), None)
        if warn:
            warnings.append(warn)
        value_font, warn = self._load_font(value_font_spec) if value_font_spec else (ImageFont.load_default(), None)
        if warn:
            warnings.append(warn)

        column_spacing = self._mm_to_px(layout_spec.column_spacing_mm)
        max_label_width = 0
        for label, _ in field_rows:
            width, _ = label_font.getsize(f"{label}:")
            max_label_width = max(max_label_width, width)
        value_x = margin_left + max_label_width + self._mm_to_px(2)
        column_break = margin_left + content_width / 2 + column_spacing / 2
        right_column_x = max(column_break, value_x + self._mm_to_px(5))

        rows_per_column = math.ceil(len(field_rows) / 2) if len(field_rows) > 6 else len(field_rows)
        left_rows = field_rows[:rows_per_column]
        right_rows = field_rows[rows_per_column:]

        def draw_rows(rows: Sequence[Tuple[str, str]], start_x: int, baseline_y: int) -> int:
            y_pos = baseline_y
            for label, value in rows:
                label_text = f"{label}:"
                draw.text((start_x, y_pos), label_text, fill=0, font=label_font)
                lw, lh = label_font.getsize(label_text)
                draw.text((start_x + max_label_width + self._mm_to_px(2), y_pos), value, fill=0, font=value_font)
                _, vh = value_font.getsize(value)
                row_height = max(lh, vh)
                y_pos += row_height + self._mm_to_px(layout_spec.field_gap_mm)
            return y_pos

        left_end = draw_rows(left_rows, margin_left, current_y)
        right_start_y = current_y
        right_end = draw_rows(right_rows, int(right_column_x), right_start_y)
        current_y = max(left_end, right_end)

        current_y += self._mm_to_px(layout_spec.section_gap_mm)

        price_lines = self._format_price_lines(item)
        price_font_spec = self.settings.fonts.get("price")
        price_font, warn = self._load_font(price_font_spec) if price_font_spec else (ImageFont.load_default(), None)
        if warn:
            warnings.append(warn)
        msrp_font_spec = self.settings.fonts.get("msrp") or price_font_spec
        msrp_font, warn = self._load_font(msrp_font_spec) if msrp_font_spec else (price_font, None)
        if warn:
            warnings.append(warn)
        for index, line in enumerate(price_lines):
            font = msrp_font if line.startswith("MSRP") else price_font
            tw, th = font.getsize(line)
            draw.text((margin_left + (content_width - tw) / 2, current_y), line, fill=0, font=font)
            current_y += th + self._mm_to_px(layout_spec.field_gap_mm)

        sku_value = self._sku_value(item)
        if sku_value:
            sku_spec = self.settings.fonts.get("sku")
            sku_font, warn = self._load_font(sku_spec) if sku_spec else (ImageFont.load_default(), None)
            if warn:
                warnings.append(warn)
            _, text_height = sku_font.getsize(sku_value)
            y_position = height_px - margin_bottom - text_height
            draw.text((margin_left, y_position), f"SKU {sku_value}", fill=0, font=sku_font)

        return RenderResult(image=image, warnings=warnings)

    def render_preview(self, item: Dict[str, object], max_width: int = 360) -> RenderResult:
        if not PIL_AVAILABLE:
            raise RuntimeError(PIL_IMPORT_MESSAGE)
        result = self.render(item)
        width, height = result.image.size
        if width > max_width:
            ratio = max_width / float(width)
            new_size = (max_width, int(height * ratio))
            preview = result.image.resize(new_size, Image.LANCZOS)
        else:
            preview = result.image.copy()
        return RenderResult(image=preview, warnings=result.warnings)

    def export_pdf(self, items: Sequence[Dict[str, object]], path: str) -> List[str]:
        if not PIL_AVAILABLE:
            raise RuntimeError(PIL_IMPORT_MESSAGE)
        warnings: List[str] = []
        pages: List[Any] = []
        for item in items:
            result = self.render(item)
            pages.append(result.image)
            warnings.extend(result.warnings)
        if not pages:
            raise ValueError("No labels were available to export to PDF.")
        first, *rest = pages
        first.save(path, "PDF", resolution=self.settings.dpi, save_all=bool(rest), append_images=rest)
        return warnings

    def export_png(self, item: Dict[str, object], path: str) -> List[str]:
        if not PIL_AVAILABLE:
            raise RuntimeError(PIL_IMPORT_MESSAGE)
        result = self.render(item)
        result.image.save(path, "PNG", dpi=(self.settings.dpi, self.settings.dpi))
        return result.warnings

    def print_to_default(self, item: Dict[str, object]) -> List[str]:
        if not PIL_AVAILABLE:
            raise RuntimeError(PIL_IMPORT_MESSAGE)
        result = self.render(item)
        warnings = list(result.warnings)
        if os.name != "nt":
            warnings.append("Direct printing is only supported on Windows.")
            return warnings
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, "dymo_label.png")
        result.image.save(temp_path, "PNG", dpi=(self.settings.dpi, self.settings.dpi))
        try:
            os.startfile(temp_path, "print")  # type: ignore[attr-defined]
        except OSError as exc:
            warnings.append(f"Failed to start printing: {exc}")
        return warnings


__all__ = [
    "DymoLabelRenderer",
    "RenderResult",
    "PIL_AVAILABLE",
    "PIL_IMPORT_MESSAGE",
    "ensure_pillow",
]
