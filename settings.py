import json
import os
from dataclasses import dataclass
from typing import Dict, Optional

import db


DEFAULT_SETTINGS_PATH = db.resource_path("settings.json")

DEFAULT_CONFIG = {
    "dymo_label": {
        "pdf_reference": "rdlcBarcodePrintingDymoVerticleWithMsrp.pdf",
        "width_mm": 57.15,
        "height_mm": 190.5,
        "dpi": 300,
        "margins_mm": {
            "top": 1.5,
            "right": 1.5,
            "bottom": 1.5,
            "left": 1.5,
        },
        "barcode": {
            "narrow_bar_mm": 0.25,
            "wide_bar_mm": 0.64,
            "height_mm": 30.0,
            "quiet_zone_mm": 2.0,
            "text_gap_mm": 2.5,
        },
        "layout": {
            "collection_gap_mm": 3.5,
            "field_gap_mm": 2.4,
            "section_gap_mm": 4.5,
            "column_spacing_mm": 18.0,
        },
        "fonts": {
            "collection": {"name": "arialbd.ttf", "size_pt": 24},
            "field_label": {"name": "arial.ttf", "size_pt": 12},
            "field_value": {"name": "arialbd.ttf", "size_pt": 14},
            "price": {"name": "arialbd.ttf", "size_pt": 18},
            "msrp": {"name": "arial.ttf", "size_pt": 14},
            "sku": {"name": "arial.ttf", "size_pt": 10},
        },
    }
}


@dataclass
class FontSpec:
    name: str
    size_pt: int


@dataclass
class BarcodeSpec:
    narrow_bar_mm: float
    wide_bar_mm: float
    height_mm: float
    quiet_zone_mm: float
    text_gap_mm: float


@dataclass
class MarginSpec:
    top: float
    right: float
    bottom: float
    left: float


@dataclass
class LayoutSpec:
    collection_gap_mm: float
    field_gap_mm: float
    section_gap_mm: float
    column_spacing_mm: float


@dataclass
class DymoLabelSettings:
    pdf_reference: Optional[str]
    width_mm: float
    height_mm: float
    dpi: int
    margins: MarginSpec
    barcode: BarcodeSpec
    fonts: Dict[str, FontSpec]
    layout: LayoutSpec


def _ensure_default_settings(path: str) -> Dict[str, Dict]:
    if not os.path.exists(path):
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(DEFAULT_CONFIG, handle, indent=2)
        return json.loads(json.dumps(DEFAULT_CONFIG))
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data


def _parse_font(name: str, data: Dict[str, object]) -> FontSpec:
    return FontSpec(name=data.get("name", name), size_pt=int(data.get("size_pt", 12)))


def load_settings(path: str = DEFAULT_SETTINGS_PATH) -> DymoLabelSettings:
    data = _ensure_default_settings(path)
    dymo = data.get("dymo_label", {})
    margins = dymo.get("margins_mm", {})
    barcode = dymo.get("barcode", {})
    fonts = {
        key: _parse_font(key, value)
        for key, value in dymo.get("fonts", {}).items()
    }
    layout = dymo.get("layout", {})
    return DymoLabelSettings(
        pdf_reference=dymo.get("pdf_reference"),
        width_mm=float(dymo.get("width_mm", 57.15)),
        height_mm=float(dymo.get("height_mm", 190.5)),
        dpi=int(dymo.get("dpi", 300)),
        margins=MarginSpec(
            top=float(margins.get("top", 1.5)),
            right=float(margins.get("right", 1.5)),
            bottom=float(margins.get("bottom", 1.5)),
            left=float(margins.get("left", 1.5)),
        ),
        barcode=BarcodeSpec(
            narrow_bar_mm=float(barcode.get("narrow_bar_mm", 0.25)),
            wide_bar_mm=float(barcode.get("wide_bar_mm", 0.64)),
            height_mm=float(barcode.get("height_mm", 30.0)),
            quiet_zone_mm=float(barcode.get("quiet_zone_mm", 2.0)),
            text_gap_mm=float(barcode.get("text_gap_mm", 2.5)),
        ),
        fonts=fonts,
        layout=LayoutSpec(
            collection_gap_mm=float(layout.get("collection_gap_mm", 3.5)),
            field_gap_mm=float(layout.get("field_gap_mm", 2.4)),
            section_gap_mm=float(layout.get("section_gap_mm", 4.5)),
            column_spacing_mm=float(layout.get("column_spacing_mm", 18.0)),
        ),
    )


__all__ = [
    "BarcodeSpec",
    "DymoLabelSettings",
    "FontSpec",
    "LayoutSpec",
    "MarginSpec",
    "load_settings",
]
