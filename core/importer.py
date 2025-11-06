"""Data import helpers for RugBase."""

from __future__ import annotations

import csv
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional

import db

FIELD_MAPPING: Dict[str, str] = {
    "RugNo": "rug_no",
    "SKU": "sku",
    "Type": "type",
    "Collection": "collection",
    "Brand": "brand",
    "VDesign": "v_design",
    "Design": "design",
    "Ground": "ground",
    "Border": "border",
    "Size": "size_label",
    "STSize": "st_size",
    "Area": "area",
    "StockLocation": "stock_location",
    "Godown": "godown",
    "PurchaseDate": "purchase_date",
    "PVNo": "pv_no",
    "Vendor": "vendor",
    "SoldOn": "sold_on",
    "InvoiceNo": "invoice_no",
    "Customer": "customer",
    "Status": "status",
    "PaymentStatus": "payment_status",
    "Notes": "notes",
}


@dataclass
class ImportResult:
    """Represents the outcome of an import operation."""

    inserted: int = 0
    updated: int = 0
    skipped: int = 0

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.skipped


class ImporterError(Exception):
    """Raised when an import process fails."""


def import_csv(path: str) -> ImportResult:
    """Import rug data from a CSV file."""

    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            records = list(reader)
    except (OSError, csv.Error) as exc:
        raise ImporterError(f"Failed to read CSV file: {exc}") from exc

    return _process_records(_iter_csv_records(records))


def import_xml(path: str) -> ImportResult:
    """Import rug data from an XML file."""

    try:
        tree = ET.parse(path)
    except (ET.ParseError, OSError) as exc:
        raise ImporterError(f"Failed to read XML file: {exc}") from exc

    root = tree.getroot()

    items = list(root.findall(".//Item"))
    if not items and root.tag.lower() == "item":
        items = [root]

    def _xml_records() -> Iterator[Dict[str, str]]:
        for element in items:
            record: Dict[str, str] = {}
            for child in element:
                record[child.tag] = child.text.strip() if child.text else ""
            yield record

    return _process_records(_xml_records())


def _iter_csv_records(records: Iterable[Dict[str, str]]) -> Iterator[Dict[str, str]]:
    for record in records:
        if not any(value.strip() for key, value in record.items() if isinstance(key, str) and isinstance(value, str)):
            continue
        cleaned: Dict[str, str] = {}
        for key, value in record.items():
            if not isinstance(key, str) or key == "":
                continue
            if isinstance(value, str):
                cleaned[key] = value.strip()
            elif value is None:
                cleaned[key] = ""
            else:
                cleaned[key] = str(value).strip()
        yield cleaned


def _process_records(records: Iterable[Dict[str, str]]) -> ImportResult:
    result = ImportResult()

    for source in records:
        mapped = _map_source_to_item(source)
        if not mapped:
            result.skipped += 1
            continue
        item_id, created = db.upsert_item(mapped)
        if created:
            result.inserted += 1
        else:
            result.updated += 1

    return result


def _map_source_to_item(source: Dict[str, str]) -> Dict[str, Any]:
    item: Dict[str, Any] = {}
    for source_field, target_field in FIELD_MAPPING.items():
        if source_field not in source:
            continue
        value = source.get(source_field, "").strip()
        if value == "":
            # Treat empty strings as missing data to avoid overwriting existing values.
            continue

        if target_field == "area":
            item[target_field] = _parse_float(value)
        elif target_field in {"purchase_date", "sold_on"}:
            item[target_field] = _normalize_date(value)
        else:
            item[target_field] = value

    return item


def _parse_float(value: str) -> Optional[float]:
    normalized = value.replace(",", "")
    try:
        return float(normalized)
    except ValueError:
        return None


def _normalize_date(value: str) -> Optional[str]:
    value = value.strip()
    if not value:
        return None

    # Try ISO formats first (YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS)
    for parser in (_parse_iso_datetime, _parse_known_formats):
        parsed = parser(value)
        if parsed:
            return parsed

    return None


def _parse_iso_datetime(value: str) -> Optional[str]:
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    return dt.date().isoformat()


def _parse_known_formats(value: str) -> Optional[str]:
    formats = [
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%d.%m.%Y",
        "%m.%d.%Y",
        "%d %b %Y",
        "%d %B %Y",
        "%b %d, %Y",
        "%B %d, %Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.date().isoformat()
        except ValueError:
            continue
    return None
