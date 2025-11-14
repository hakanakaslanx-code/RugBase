from __future__ import annotations

import math
import os
import re
import subprocess
import sys
import tempfile
import zlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

Image = None  # type: ignore[assignment]
ImageDraw = None  # type: ignore[assignment]
ImageFont = None  # type: ignore[assignment]
PIL_IMPORT_MESSAGE = (
    "Pillow (PIL) is required. Install it to enable label preview and printing,"
    " then restart the application."
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


def ensure_pillow() -> bool:
    """Ensure Pillow can be imported without triggering runtime installation."""

    global PIL_AVAILABLE, PIL_IMPORT_MESSAGE
    if PIL_AVAILABLE:
        return True

    if _import_pillow():
        PIL_AVAILABLE = True
        PIL_IMPORT_MESSAGE = ""
        return True

    PIL_IMPORT_MESSAGE = (
        "Pillow (PIL) is required. Use the 'Install Pillow' option to complete the"
        " installation and restart the application afterwards."
    )
    return False


def install_pillow() -> Tuple[bool, str]:
    """Attempt to install Pillow via pip and return the outcome."""

    command = [sys.executable, "-m", "pip", "install", "Pillow"]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except Exception as exc:  # pragma: no cover - subprocess availability
        return False, str(exc)
    output_parts = [part.strip() for part in (completed.stdout, completed.stderr) if part.strip()]
    details = "\n".join(output_parts)
    if completed.returncode == 0:
        ensure_pillow()
        return True, details
    return False, details


def measure_text(draw: "ImageDraw.ImageDraw", text: str, font: Any) -> Tuple[int, int]:
    """Measure text dimensions with compatibility across Pillow versions."""

    if not text:
        return 0, 0
    try:
        bbox = draw.textbbox((0, 0), text, font=font)
    except AttributeError:
        bbox = None
    if bbox:
        width = bbox[2] - bbox[0]
        height = bbox[3] - bbox[1]
        return int(width), int(height)

    width = 0
    if hasattr(draw, "textlength"):
        try:
            width = int(math.ceil(draw.textlength(text, font=font)))
        except TypeError:
            width = 0
    if not width and hasattr(font, "getlength"):
        try:
            width = int(math.ceil(font.getlength(text)))
        except TypeError:
            width = 0

    height = 0
    if hasattr(font, "getbbox"):
        bbox = font.getbbox(text)
        width = max(width, int(bbox[2] - bbox[0]))
        height = int(bbox[3] - bbox[1])
    else:
        mask = font.getmask(text)
        mask_width, mask_height = mask.size
        width = max(width, int(mask_width))
        height = int(mask_height)

    if not width and text:
        width = len(text)
    if height <= 0 and hasattr(font, "getmetrics"):
        ascent, descent = font.getmetrics()
        height = int(ascent + descent)
    height = max(height, 1)
    return int(width), int(height)


import db
from settings import DymoLabelSettings, FontSpec, load_settings

INCH_TO_MM = 25.4
POINTS_PER_INCH = 72

# Fixed canvas dimensions for the portrait Dymo 30336 label at 300 DPI.
CANVAS_WIDTH_PX = 300
CANVAS_HEIGHT_PX = 638


@dataclass
class RenderResult:
    image: Any
    warnings: List[str]


class Code128:
    """Minimal Code128 subset B encoder for alphanumeric rug identifiers."""

    START_B = 104
    STOP = 106

    CODE_PATTERNS: Sequence[str] = (
        "212222",
        "222122",
        "222221",
        "121223",
        "121322",
        "131222",
        "122213",
        "122312",
        "132212",
        "221213",
        "221312",
        "231212",
        "112232",
        "122132",
        "122231",
        "113222",
        "123122",
        "123221",
        "223211",
        "221132",
        "221231",
        "213212",
        "223112",
        "312131",
        "311222",
        "321122",
        "321221",
        "312212",
        "322112",
        "322211",
        "212123",
        "212321",
        "232121",
        "111323",
        "131123",
        "131321",
        "112313",
        "132113",
        "132311",
        "211313",
        "231113",
        "231311",
        "112133",
        "112331",
        "132131",
        "113123",
        "113321",
        "133121",
        "313121",
        "211331",
        "231131",
        "213113",
        "213311",
        "213131",
        "311123",
        "311321",
        "331121",
        "312113",
        "312311",
        "332111",
        "314111",
        "221411",
        "431111",
        "111224",
        "111422",
        "121124",
        "121421",
        "141122",
        "141221",
        "112214",
        "112412",
        "122114",
        "122411",
        "142112",
        "142211",
        "241211",
        "221114",
        "413111",
        "241112",
        "134111",
        "111242",
        "121142",
        "121241",
        "114212",
        "124112",
        "124211",
        "411212",
        "421112",
        "421211",
        "212141",
        "214121",
        "412121",
        "111143",
        "111341",
        "131141",
        "114113",
        "114311",
        "411113",
        "411311",
        "113141",
        "114131",
        "311141",
        "411131",
        "211412",
        "211214",
        "211232",
        "2331112",
    )

    CODE_TO_CHAR_B: Dict[int, str] = {index: chr(index + 32) for index in range(95)}
    CHAR_TO_CODE_B: Dict[str, int] = {value: key for key, value in CODE_TO_CHAR_B.items()}

    def __init__(self, module_px: int) -> None:
        self.module_px = max(1, module_px)

    @staticmethod
    def measure(widths: Sequence[int]) -> int:
        return sum(abs(value) for value in widths)

    def encode(self, data: str) -> List[int]:
        if not data:
            raise ValueError("Code128 requires at least one character.")
        codes: List[int] = [self.START_B]
        for char in data:
            code = self.CHAR_TO_CODE_B.get(char)
            if code is None:
                raise ValueError(f"Unsupported Code128 character: {char!r}")
            codes.append(code)
        checksum = (codes[0] + sum((index * value) for index, value in enumerate(codes[1:], start=1))) % 103
        codes.append(checksum)
        codes.append(self.STOP)
        widths: List[int] = []
        for code in codes:
            pattern = self.CODE_PATTERNS[code]
            modules = [int(ch) for ch in pattern]
            for position, module_width in enumerate(modules):
                pixels = module_width * self.module_px
                widths.append(pixels if position % 2 == 0 else -pixels)
        return widths

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


def _format_pdf_number(value: float) -> str:
    text = f"{value:.4f}"
    text = text.rstrip("0").rstrip(".")
    return text or "0"


def _px_to_points(pixels: int, dpi: int) -> float:
    return pixels / float(dpi) * POINTS_PER_INCH


def _encode_pdf_page(image: "Image.Image", dpi: int) -> Tuple[Dict[str, Any], bytes]:
    mode = image.mode
    if mode not in {"L", "RGB"}:
        image = image.convert("L")
        mode = image.mode
    width_px, height_px = image.size
    raw = image.tobytes()
    compressed = zlib.compress(raw)
    resources: Dict[str, Any] = {
        "width_px": width_px,
        "height_px": height_px,
        "mode": mode,
        "width_pt": _px_to_points(width_px, dpi),
        "height_pt": _px_to_points(height_px, dpi),
    }
    return resources, compressed


def _write_pdf(images: Sequence["Image.Image"], dpi: int, path: str) -> None:
    if not images:
        raise ValueError("No images were provided for PDF export.")

    buffer = bytearray()
    offsets: List[int] = []

    def add_object(index: int, content: bytes) -> None:
        offsets.append(len(buffer))
        buffer.extend(f"{index} 0 obj\n".encode("ascii"))
        buffer.extend(content)
        if not content.endswith(b"\n"):
            buffer.extend(b"\n")
        buffer.extend(b"endobj\n")

    buffer.extend(b"%PDF-1.4\n%\xE2\xE3\xCF\xD3\n")

    page_entries: List[int] = []
    object_index = 3

    page_objects: List[Tuple[int, int, int]] = []
    resource_payloads: List[Tuple[Dict[str, Any], bytes, str]] = []

    for page_number, image in enumerate(images, start=1):
        resources, payload = _encode_pdf_page(image, dpi)
        image_object = object_index
        content_object = object_index + 1
        page_object = object_index + 2
        object_index += 3
        name = f"Im{page_number}"
        resource_payloads.append((resources, payload, name))
        page_objects.append((image_object, content_object, page_object))
        page_entries.append(page_object)

    add_object(
        1,
        (
            b"<< /Type /Catalog /Pages 2 0 R /ViewerPreferences "
            b"<< /PrintScaling /None /PickTrayByPDFSize true /AutoRotate false >> >>\n"
        ),
    )

    kids = " ".join(f"{entry} 0 R" for entry in page_entries)
    add_object(2, f"<< /Type /Pages /Count {len(page_entries)} /Kids [{kids}] >>\n".encode("ascii"))

    for (image_object, content_object, page_object), (resources, payload, name) in zip(page_objects, resource_payloads):
        width_px = resources["width_px"]
        height_px = resources["height_px"]
        width_pt = resources["width_pt"]
        height_pt = resources["height_pt"]
        mode = resources["mode"]
        procset = "/ImageB" if mode == "L" else "/ImageC"
        colorspace = "/DeviceGray" if mode == "L" else "/DeviceRGB"

        image_dict = (
            f"<< /Type /XObject /Subtype /Image /Width {width_px} /Height {height_px} "
            f"/ColorSpace {colorspace} /BitsPerComponent 8 /Filter /FlateDecode /Length {len(payload)} >>\n".encode(
                "ascii"
            )
        )
        add_object(image_object, image_dict + b"stream\n" + payload + b"\nendstream\n")

        content_stream = (
            f"q {_format_pdf_number(width_pt)} 0 0 {_format_pdf_number(height_pt)} 0 0 cm /{name} Do Q\n".encode(
                "ascii"
            )
        )
        add_object(content_object, f"<< /Length {len(content_stream)} >>\n".encode("ascii") + b"stream\n" + content_stream + b"endstream\n")

        media_box = (
            f"[0 0 {_format_pdf_number(width_pt)} {_format_pdf_number(height_pt)}]"
        )
        page_dict = (
            "<< /Type /Page /Parent 2 0 R "
            f"/MediaBox {media_box} /CropBox {media_box} /Rotate 0 "
            f"/Resources << /ProcSet [/PDF {procset}] /XObject << /{name} {image_object} 0 R >> >> "
            f"/Contents {content_object} 0 R >>\n"
        ).encode("ascii")
        add_object(page_object, page_dict)

    xref_position = len(buffer)
    total_objects = object_index - 1
    buffer.extend(f"xref\n0 {total_objects + 1}\n".encode("ascii"))
    buffer.extend(b"0000000000 65535 f \n")
    for offset in offsets:
        buffer.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    buffer.extend(f"trailer\n<< /Size {total_objects + 1} /Root 1 0 R >>\n".encode("ascii"))
    buffer.extend(f"startxref\n{xref_position}\n%%EOF\n".encode("ascii"))

    with open(path, "wb") as handle:
        handle.write(buffer)


class DymoLabelRenderer:
    def __init__(self) -> None:
        ensure_pillow()
        self.settings: DymoLabelSettings = load_settings()
        self._font_cache: Dict[Tuple[str, int], Any] = {}
        self._pdf_dimensions: Optional[Tuple[float, float]] = None
        self._force_default_font = False
        self._default_font_warning: Optional[str] = None

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

    def use_default_font_fallback(self, warning: Optional[str] = None) -> None:
        """Force the renderer to use Pillow's built-in bitmap font."""

        if not PIL_AVAILABLE:
            return
        self._force_default_font = True
        self._default_font_warning = (
            warning
            or "Packaged label fonts were not found. Using Pillow's default font for rendering."
        )
        self._font_cache.clear()

    def _use_default_font(self, spec: FontSpec, warning: Optional[str]) -> Tuple[Any, Optional[str]]:
        key = (spec.name, spec.size_pt)
        cached = self._font_cache.get(key)
        if cached:
            return cached, None
        font = ImageFont.load_default()
        self._font_cache[key] = font
        return font, warning or (
            "TrueType fonts were unavailable. Using Pillow's default font."
        )

    def _load_font(self, spec: FontSpec) -> Tuple[Any, Optional[str]]:
        if not PIL_AVAILABLE:
            raise RuntimeError(PIL_IMPORT_MESSAGE)
        key = (spec.name, spec.size_pt)
        cached = self._font_cache.get(key)
        if cached:
            return cached, None
        if self._force_default_font:
            warning = self._default_font_warning
            return self._use_default_font(spec, warning)
        size_px = self._pt_to_px(spec.size_pt)
        try:
            font = ImageFont.truetype(spec.name, size_px)
        except OSError:
            missing_warning = f"TrueType font '{spec.name}' could not be found."
            try:
                font = ImageFont.truetype("arial.ttf", size_px)
            except OSError:
                default_warning = (
                    f"{missing_warning} Arial was also unavailable. Using Pillow's default font."
                )
                self.use_default_font_fallback(default_warning)
                return self._use_default_font(spec, default_warning)
            else:
                fallback_warning = f"{missing_warning} Using Arial as a fallback where available."
                self._font_cache[key] = font
                return font, fallback_warning
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
        """Return the fixed canvas dimensions for the label renderer."""

        return CANVAS_WIDTH_PX, CANVAS_HEIGHT_PX

    # endregion

    def _normalize_output_image(self, image: Any, warnings: List[str]) -> Any:
        if not PIL_AVAILABLE:
            return image
        expected_width, expected_height = self._canvas_size()
        width, height = image.size
        dpi = image.info.get("dpi", (self.settings.dpi, self.settings.dpi))
        expected_width_in = self.settings.width_mm / INCH_TO_MM
        expected_height_in = self.settings.height_mm / INCH_TO_MM
        expected_text = f"{expected_width_in:.2f}\" × {expected_height_in:.2f}\""
        if (width, height) == (expected_width, expected_height):
            return image
        if (width, height) == (expected_height, expected_width) or width > height:
            rotated = image.rotate(90, expand=True)
            rotated.info["dpi"] = dpi
            warnings.append(
                "Label orientation corrected to portrait before output."
            )
            width, height = rotated.size
            if (width, height) != (expected_width, expected_height):
                warnings.append(
                    f"Label size adjusted for portrait printing. Expected {expected_width}×{expected_height}px"
                    f" (~{expected_text})."
                )
            return rotated
        warnings.append(
            "Label dimensions differed from the expected portrait layout. Output may not align with the printer"
            f" (expected {expected_width}×{expected_height}px ≈ {expected_text})."
        )
        return image

    def _clean_value(self, value: Optional[object]) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        return text

    def _collection_value(self, item: Dict[str, object]) -> str:
        for key in ("collection", "Collection", "v_collection", "VCollection"):
            text = self._clean_value(item.get(key))
            if text:
                return text
        return ""

    def _barcode_value(self, item: Dict[str, object]) -> Optional[str]:
        for key in ("rug_no", "RugNo", "roll_no", "RollNo"):
            text = self._clean_value(item.get(key))
            if text:
                return text.upper()
        return None

    def _price_text(self, item: Dict[str, object]) -> Optional[str]:
        primary = self._clean_value(item.get("sp") or item.get("SP") or item.get("price") or item.get("Price"))
        fallback = self._clean_value(item.get("msrp") or item.get("MSRP"))
        amount = primary or fallback
        if not amount:
            return None
        return f"Price $ {amount}"

    def _compose_field_rows(self, item: Dict[str, object]) -> List[Tuple[str, str]]:
        rows: List[Tuple[str, str]] = []

        design_value = self._clean_value(
            item.get("design")
            or item.get("Design")
            or item.get("v_design")
            or item.get("VDesign")
        )
        if design_value:
            rows.append(("Design", design_value))

        ground = self._clean_value(item.get("ground") or item.get("Ground"))
        border = self._clean_value(item.get("border") or item.get("Border"))
        if ground or border:
            color_value = f"{ground}/{border}" if ground and border else ground or border
            rows.append(("Color", color_value))

        actual_size = self._clean_value(item.get("a_size") or item.get("ASize"))
        standard_size = self._clean_value(item.get("st_size") or item.get("StSize"))
        size_value = ""
        if actual_size and standard_size and actual_size.lower() != standard_size.lower():
            size_value = f"{actual_size} / {standard_size}"
        else:
            size_value = actual_size or standard_size
        if size_value:
            rows.append(("Size", size_value))

        origin_value = self._clean_value(item.get("origin") or item.get("Origin"))
        if origin_value:
            rows.append(("Origin", origin_value))

        content_value = self._clean_value(item.get("content") or item.get("Content"))
        if not content_value:
            type_value = self._clean_value(item.get("type") or item.get("Type"))
            if type_value:
                lowered = type_value.lower()
                if "hand" in lowered:
                    content_value = "Hand Made"
                elif "machine" in lowered:
                    content_value = "Machine Made"
                else:
                    content_value = type_value
        if content_value:
            rows.append(("Content", content_value))

        rug_value = self._barcode_value(item)
        if rug_value:
            rows.append(("Rug #", rug_value))

        return rows

    def render(self, item: Dict[str, object]) -> RenderResult:
        if not PIL_AVAILABLE:
            raise RuntimeError(PIL_IMPORT_MESSAGE)

        width_px, height_px = self._canvas_size()
        image = Image.new("L", (width_px, height_px), color=255)
        image.info["dpi"] = (self.settings.dpi, self.settings.dpi)
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
        module_px = max(1, narrow_bar_px)
        quiet_zone_px = max(self._mm_to_px(barcode_spec.quiet_zone_mm), module_px * 6)
        barcode_height_px = self._mm_to_px(barcode_spec.height_mm)
        barcode_gap_px = self._mm_to_px(barcode_spec.text_gap_mm)
        field_gap_px = self._mm_to_px(layout_spec.field_gap_mm)
        collection_gap_px = self._mm_to_px(layout_spec.collection_gap_mm)
        label_spacing_px = max(1, self._mm_to_px(0.8))
        section_gap_px = self._mm_to_px(layout_spec.section_gap_mm)

        barcode = Code128(module_px)

        price_font_spec = self.settings.fonts.get("price")
        price_font, warn = self._load_font(price_font_spec) if price_font_spec else (ImageFont.load_default(), None)
        if warn:
            warnings.append(warn)

        collection_spec = self.settings.fonts.get("collection")
        collection_font, warn = self._load_font(collection_spec) if collection_spec else (ImageFont.load_default(), None)
        if warn:
            warnings.append(warn)

        label_font_spec = self.settings.fonts.get("field_label")
        value_font_spec = self.settings.fonts.get("field_value")
        label_font, warn = self._load_font(label_font_spec) if label_font_spec else (ImageFont.load_default(), None)
        if warn:
            warnings.append(warn)
        value_font, warn = self._load_font(value_font_spec) if value_font_spec else (ImageFont.load_default(), None)
        if warn:
            warnings.append(warn)

        current_y = margin_top

        price_text = self._price_text(item)
        if price_text:
            price_width, price_height = measure_text(draw, price_text, price_font)
            draw.text(
                (
                    margin_left + (content_width - price_width) / 2,
                    current_y,
                ),
                price_text,
                fill=0,
                font=price_font,
            )
            current_y += price_height + barcode_gap_px
        else:
            warnings.append("Price value is missing; price line omitted from label.")

        barcode_value = self._barcode_value(item)
        if barcode_value:
            try:
                widths = barcode.encode(barcode_value)
            except ValueError as exc:
                warnings.append(f"Barcode could not be created: {exc}")
            else:
                pattern_width = Code128.measure(widths)
                total_width = pattern_width + quiet_zone_px * 2
                extra_space = content_width - total_width
                if extra_space < 0:
                    warnings.append(
                        "Barcode width exceeds printable area; it will extend into the margins."
                    )
                    extra_space = 0
                start_x = margin_left + (extra_space // 2) + quiet_zone_px
                start_y = current_y
                barcode.draw(draw, (start_x, start_y), barcode_height_px, widths)
                current_y += barcode_height_px + section_gap_px
        else:
            warnings.append("Rug # is empty. Barcode generation was skipped.")

        collection_value = self._collection_value(item)
        if collection_value:
            text_width, text_height = measure_text(draw, collection_value, collection_font)
            draw.text(
                (
                    margin_left + (content_width - text_width) / 2,
                    current_y,
                ),
                collection_value,
                fill=0,
                font=collection_font,
            )
            current_y += text_height + collection_gap_px

        field_rows = self._compose_field_rows(item)
        if field_rows:
            max_label_width = 0
            max_value_width = 0
            label_metrics: List[Tuple[int, int]] = []
            value_metrics: List[Tuple[int, int]] = []
            for label, value in field_rows:
                label_text = f"{label} :"
                lw, lh = measure_text(draw, label_text, label_font)
                vw, vh = measure_text(draw, value, value_font)
                label_metrics.append((lw, lh))
                value_metrics.append((vw, vh))
                max_label_width = max(max_label_width, lw)
                max_value_width = max(max_value_width, vw)

            block_width = max_label_width + label_spacing_px + max_value_width
            start_x = margin_left + max(0, (content_width - block_width) // 2)

            for index, (label, value) in enumerate(field_rows):
                label_text = f"{label} :"
                lw, lh = label_metrics[index]
                vw, vh = value_metrics[index]
                draw.text((start_x, current_y), label_text, fill=0, font=label_font)
                draw.text((start_x + max_label_width + label_spacing_px, current_y), value, fill=0, font=value_font)
                row_height = max(lh, vh)
                current_y += row_height
                if index != len(field_rows) - 1:
                    current_y += field_gap_px

        current_y = min(current_y, height_px - margin_bottom)

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
            normalized = self._normalize_output_image(result.image, warnings)
            pages.append(normalized)
            warnings.extend(result.warnings)
        if not pages:
            raise ValueError("No labels were available to export to PDF.")
        _write_pdf(pages, self.settings.dpi, path)
        return warnings

    def export_png(self, item: Dict[str, object], path: str) -> List[str]:
        if not PIL_AVAILABLE:
            raise RuntimeError(PIL_IMPORT_MESSAGE)
        result = self.render(item)
        normalized = self._normalize_output_image(result.image, result.warnings)
        normalized.save(path, "PNG", dpi=(self.settings.dpi, self.settings.dpi))
        return result.warnings

    def print_to_default(self, item: Dict[str, object]) -> List[str]:
        if not PIL_AVAILABLE:
            raise RuntimeError(PIL_IMPORT_MESSAGE)
        result = self.render(item)
        warnings = list(result.warnings)
        normalized = self._normalize_output_image(result.image, warnings)
        if os.name != "nt":
            warnings.append("Direct printing is only supported on Windows.")
            return warnings
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, "dymo_label.png")
        normalized.save(temp_path, "PNG", dpi=(self.settings.dpi, self.settings.dpi))
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
    "install_pillow",
    "measure_text",
]
