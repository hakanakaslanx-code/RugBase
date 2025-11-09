import json
import logging
import os
import shutil
from dataclasses import dataclass
from typing import Dict, Optional

import db
import dependency_loader
from core import app_paths


logger = logging.getLogger(__name__)


DEFAULT_SETTINGS_PATH = db.data_path("settings.json")

DEFAULT_SPREADSHEET_ID = "1n6_7L-8fPtQBN_QodxBXj3ZMzOPpMzdx8tpdRZZe5F8"
DEFAULT_SERVICE_ACCOUNT_EMAIL = "rugbase-sync@rugbase-sync.iam.gserviceaccount.com"
DEFAULT_WORKSHEET_TITLE = "items"
SYNC_SETTINGS_PATH = db.data_path("sync_settings.json")
_CREDENTIALS_TARGET = app_paths.credentials_path("credentials.json")
_BUNDLED_CREDENTIALS = dependency_loader.default_credentials_path("credentials.json")
if _BUNDLED_CREDENTIALS is not None and not _CREDENTIALS_TARGET.exists():
    try:
        shutil.copy2(_BUNDLED_CREDENTIALS, _CREDENTIALS_TARGET)
    except OSError as exc:  # pragma: no cover - depends on filesystem state
        logger.debug("Bundled credentials could not be copied: %s", exc, exc_info=True)

DEFAULT_CREDENTIALS_PATH = str(_CREDENTIALS_TARGET)

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


@dataclass
class GoogleSyncSettings:
    spreadsheet_id: str
    credential_path: str
    service_account_email: str = DEFAULT_SERVICE_ACCOUNT_EMAIL
    worksheet_title: str = DEFAULT_WORKSHEET_TITLE


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


def _ensure_sync_settings(path: str = SYNC_SETTINGS_PATH) -> Dict[str, str]:
    default_settings = {
        "spreadsheet_id": DEFAULT_SPREADSHEET_ID,
        "credential_path": DEFAULT_CREDENTIALS_PATH,
        "service_account_email": DEFAULT_SERVICE_ACCOUNT_EMAIL,
        "worksheet_title": DEFAULT_WORKSHEET_TITLE,
    }
    if not os.path.exists(path):
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(default_settings, handle, indent=2)
        return json.loads(json.dumps(default_settings))

    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    merged = dict(default_settings)
    merged.update({key: value for key, value in data.items() if isinstance(value, str)})
    return merged


def load_google_sync_settings(path: str = SYNC_SETTINGS_PATH) -> GoogleSyncSettings:
    data = _ensure_sync_settings(path)
    return GoogleSyncSettings(
        spreadsheet_id=data.get("spreadsheet_id", DEFAULT_SPREADSHEET_ID),
        credential_path=data.get("credential_path", DEFAULT_CREDENTIALS_PATH),
        service_account_email=data.get(
            "service_account_email", DEFAULT_SERVICE_ACCOUNT_EMAIL
        ),
        worksheet_title=data.get("worksheet_title", DEFAULT_WORKSHEET_TITLE),
    )


def save_google_sync_settings(
    settings: GoogleSyncSettings, path: str = SYNC_SETTINGS_PATH
) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    payload = {
        "spreadsheet_id": settings.spreadsheet_id,
        "credential_path": settings.credential_path,
        "service_account_email": settings.service_account_email,
        "worksheet_title": settings.worksheet_title,
    }

    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


__all__ = [
    "BarcodeSpec",
    "DymoLabelSettings",
    "GoogleSyncSettings",
    "FontSpec",
    "LayoutSpec",
    "MarginSpec",
    "DEFAULT_SPREADSHEET_ID",
    "DEFAULT_SERVICE_ACCOUNT_EMAIL",
    "DEFAULT_WORKSHEET_TITLE",
    "DEFAULT_CREDENTIALS_PATH",
    "load_settings",
    "load_google_sync_settings",
    "save_google_sync_settings",
]
