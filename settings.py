"""Application configuration helpers for RugBase."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional

from core import app_paths


logger = logging.getLogger(__name__)


DEFAULT_SETTINGS_PATH = str(app_paths.data_path("settings.json"))

DEFAULT_SPREADSHEET_ID = os.getenv(
    "RUGBASE_SPREADSHEET_ID",
    str(app_paths.data_path("rugbase_inventory.xlsx")),
)
DEFAULT_SERVICE_ACCOUNT_EMAIL = os.getenv("RUGBASE_SERVICE_ACCOUNT_EMAIL", "")
DEFAULT_WORKSHEET_TITLE = "items"
SYNC_SETTINGS_PATH = str(app_paths.data_path("sync_settings.json"))
DEFAULT_CREDENTIALS_PATH = os.getenv(
    "RUGBASE_CREDENTIALS_PATH",
    str(app_paths.credentials_path("service_account.json")),
)

DEFAULT_CONFIG = {
    "dymo_label": {
        "pdf_reference": "rdlcBarcodePrintingDymo30336Portrait.pdf",
        "width_mm": 25.4,
        "height_mm": 54.0,
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
            "height_mm": 11.5,
            "quiet_zone_mm": 1.5,
            "text_gap_mm": 2.0,
        },
        "layout": {
            "collection_gap_mm": 1.6,
            "field_gap_mm": 1.2,
            "section_gap_mm": 2.4,
            "column_spacing_mm": 0.0,
        },
        "fonts": {
            "collection": {"name": "arialbd.ttf", "size_pt": 9},
            "field_label": {"name": "arial.ttf", "size_pt": 8},
            "field_value": {"name": "arial.ttf", "size_pt": 8},
            "price": {"name": "arial.ttf", "size_pt": 11},
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
    inventory_tab: str = DEFAULT_WORKSHEET_TITLE
    customers_tab: str = "Customers"
    logs_tab: str = "Logs"
    settings_tab: str = "Settings"
    column_mapping: List[ColumnMapping] = field(default_factory=list)
    minute_limit: int = 1
    sync_interval_seconds: int = 60

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
            "inventory_tab": self.worksheet_title,
            "customers_tab": self.customers_tab,
            "logs_tab": self.logs_tab,
            "settings_tab": self.settings_tab,
            "minute_limit": self.minute_limit,
            "sync_interval_seconds": self.sync_interval_seconds,
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
        "inventory_tab": DEFAULT_WORKSHEET_TITLE,
        "customers_tab": "Customers",
        "logs_tab": "Logs",
        "settings_tab": "Settings",
        "minute_limit": 1,
        "sync_interval_seconds": 60,
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
        elif key == "sync_interval_seconds":
            try:
                merged[key] = max(15, min(600, int(value)))
            except (TypeError, ValueError):
                merged[key] = default_settings[key]
        elif isinstance(value, str):
            merged[key] = value
    return merged


def _extract_title(value: object) -> str:
    if isinstance(value, str):
        return value.strip()
    return ""


def _coerce_worksheet_title(data: Mapping[str, object]) -> str:
    worksheet = _extract_title(data.get("worksheet_title"))
    if worksheet:
        return worksheet
    legacy = _extract_title(data.get("inventory_tab"))
    if legacy:
        return legacy
    return DEFAULT_WORKSHEET_TITLE


def _coerce_inventory_title(data: Mapping[str, object], worksheet_title: str) -> str:
    if worksheet_title:
        return worksheet_title
    legacy = _extract_title(data.get("inventory_tab"))
    return legacy or DEFAULT_WORKSHEET_TITLE


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

    worksheet_title = _coerce_worksheet_title(data)
    inventory_title = _coerce_inventory_title(data, worksheet_title)

    settings = GoogleSyncSettings(
        spreadsheet_id=str(data.get("spreadsheet_id", DEFAULT_SPREADSHEET_ID)),
        credential_path=str(data.get("credential_path", DEFAULT_CREDENTIALS_PATH)),
        service_account_email=str(data.get("service_account_email", DEFAULT_SERVICE_ACCOUNT_EMAIL)),
        worksheet_title=worksheet_title,
        inventory_tab=inventory_title,
        customers_tab=str(data.get("customers_tab", "Customers")),
        logs_tab=str(data.get("logs_tab", "Logs")),
        settings_tab=str(data.get("settings_tab", "Settings")),
        column_mapping=mapping_entries,
        minute_limit=int(data.get("minute_limit", 1)),
        sync_interval_seconds=int(data.get("sync_interval_seconds", 60)),
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
