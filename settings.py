"""Application configuration helpers for RugBase."""
from __future__ import annotations

import json
import logging
import os
import shutil
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional

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
        "width_mm": 78.23,
        "height_mm": 135.38,
        "dpi": 300,
        "margins_mm": {
            "top": 2.5,
            "right": 2.5,
            "bottom": 2.5,
            "left": 2.5,
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
            "barcode_text": {"name": "arialbd.ttf", "size_pt": 16},
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
class ColumnMapping:
    """Represents a database → sheet column mapping entry."""

    field: str
    header: str


@dataclass
class GoogleSyncSettings:
    spreadsheet_id: str
    credential_path: str
    service_account_email: str = DEFAULT_SERVICE_ACCOUNT_EMAIL
    worksheet_title: str = DEFAULT_WORKSHEET_TITLE
    column_mapping: List[ColumnMapping] = field(default_factory=list)
    minute_limit: int = 1

    def mapping_dict(self) -> Dict[str, str]:
        return {entry.field: entry.header for entry in self.column_mapping}

    def update_mapping(self, mapping: Mapping[str, str]) -> None:
        self.column_mapping = [ColumnMapping(field=key, header=value) for key, value in mapping.items()]

    def to_json(self) -> Dict[str, object]:
        return {
            "spreadsheet_id": self.spreadsheet_id,
            "credential_path": self.credential_path,
            "service_account_email": self.service_account_email,
            "worksheet_title": self.worksheet_title,
            "minute_limit": self.minute_limit,
            "column_mapping": [{"field": entry.field, "header": entry.header} for entry in self.column_mapping],
        }


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


def _parse_font(name: str, data: Mapping[str, object]) -> FontSpec:
    return FontSpec(name=data.get("name", name), size_pt=int(data.get("size_pt", 12)))


def load_settings(path: str = DEFAULT_SETTINGS_PATH) -> DymoLabelSettings:
    data = _ensure_default_settings(path)
    dymo = data.get("dymo_label", {})
    margins = dymo.get("margins_mm", {})
    barcode = dymo.get("barcode", {})
    fonts = {
        key: _parse_font(key, value)
        for key, value in dymo.get("fonts", {}).items()
        if isinstance(value, Mapping)
    }
    layout = dymo.get("layout", {})
    return DymoLabelSettings(
        pdf_reference=dymo.get("pdf_reference"),
        width_mm=float(dymo.get("width_mm", 78.23)),
        height_mm=float(dymo.get("height_mm", 135.38)),
        dpi=int(dymo.get("dpi", 300)),
        margins=MarginSpec(
            top=float(margins.get("top", 2.5)),
            right=float(margins.get("right", 2.5)),
            bottom=float(margins.get("bottom", 2.5)),
            left=float(margins.get("left", 2.5)),
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


def _ensure_sync_settings(path: str = SYNC_SETTINGS_PATH) -> Dict[str, object]:
    default_settings: Dict[str, object] = {
        "spreadsheet_id": DEFAULT_SPREADSHEET_ID,
        "credential_path": DEFAULT_CREDENTIALS_PATH,
        "service_account_email": DEFAULT_SERVICE_ACCOUNT_EMAIL,
        "worksheet_title": DEFAULT_WORKSHEET_TITLE,
        "minute_limit": 1,
        "column_mapping": [],
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

    merged: Dict[str, object] = dict(default_settings)
    for key, value in data.items():
        if key == "column_mapping" and isinstance(value, list):
            merged[key] = [entry for entry in value if isinstance(entry, dict)]
        elif key == "minute_limit":
            try:
                merged[key] = max(1, min(5, int(value)))
            except (TypeError, ValueError):
                merged[key] = default_settings[key]
        elif isinstance(value, str):
            merged[key] = value
    return merged


def load_google_sync_settings(path: str = SYNC_SETTINGS_PATH) -> GoogleSyncSettings:
    data = _ensure_sync_settings(path)
    mapping_entries: List[ColumnMapping] = []
    for entry in data.get("column_mapping", []):
        if not isinstance(entry, Mapping):
            continue
        field = entry.get("field")
        header = entry.get("header")
        if isinstance(field, str) and isinstance(header, str):
            mapping_entries.append(ColumnMapping(field=field, header=header))

    settings = GoogleSyncSettings(
        spreadsheet_id=str(data.get("spreadsheet_id", DEFAULT_SPREADSHEET_ID)),
        credential_path=str(data.get("credential_path", DEFAULT_CREDENTIALS_PATH)),
        service_account_email=str(data.get("service_account_email", DEFAULT_SERVICE_ACCOUNT_EMAIL)),
        worksheet_title=str(data.get("worksheet_title", DEFAULT_WORKSHEET_TITLE)),
        column_mapping=mapping_entries,
        minute_limit=int(data.get("minute_limit", 1)),
    )
    return settings


def save_google_sync_settings(settings: GoogleSyncSettings, path: str = SYNC_SETTINGS_PATH) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    payload = settings.to_json()

    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def save_column_mapping(mapping: Mapping[str, str], path: str = DEFAULT_SETTINGS_PATH) -> None:
    data = _ensure_default_settings(path)
    google_section = data.setdefault("google_sync", {})
    if not isinstance(google_section, dict):
        google_section = {}
        data["google_sync"] = google_section
    google_section["column_mapping"] = dict(mapping)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)


def load_column_mapping(path: str = DEFAULT_SETTINGS_PATH) -> Dict[str, str]:
    data = _ensure_default_settings(path)
    google_section = data.get("google_sync", {})
    if not isinstance(google_section, Mapping):
        return {}
    mapping = google_section.get("column_mapping", {})
    if not isinstance(mapping, Mapping):
        return {}
    return {key: str(value) for key, value in mapping.items() if isinstance(value, (str, int, float))}


__all__ = [
    "BarcodeSpec",
    "ColumnMapping",
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
    "load_column_mapping",
    "save_column_mapping",
]
