"""Data import helpers for RugBase."""

from __future__ import annotations

import csv
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List

import db

FIELD_MAPPING: Dict[str, str] = {
    "RugNo": "rug_no",
    "UPC": "upc",
    "RollNo": "roll_no",
    "VRugNo": "v_rug_no",
    "Vcollection": "v_collection",
    "VCollection": "v_collection",
    "Collection": "collection",
    "VDesign": "v_design",
    "Design": "design",
    "Brandname": "brand_name",
    "BrandName": "brand_name",
    "Brand": "brand_name",
    "Ground": "ground",
    "Border": "border",
    "ASize": "a_size",
    "ActualSize": "a_size",
    "StSize": "st_size",
    "STSize": "st_size",
    "Area": "area",
    "Type": "type",
    "Rate": "rate",
    "Amount": "amount",
    "Shape": "shape",
    "Style": "style",
    "ImageFileName": "image_file_name",
    "Image": "image_file_name",
    "Origin": "origin",
    "Retail": "retail",
    "SP": "sp",
    "MSRP": "msrp",
    "Cost": "cost",
}


def _normalize_field_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


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
        for header in FIELD_MAPPING.keys():
            cleaned.setdefault(header, "")
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
    if not source:
        return {}

    normalized_keys = {
        _normalize_field_name(key): key
        for key in source.keys()
        if isinstance(key, str) and key
    }

    def _get(field_name: str) -> str:
        key = normalized_keys.get(_normalize_field_name(field_name))
        if key is None:
            return ""
        value = source.get(key, "")
        if isinstance(value, str):
            return value.strip()
        if value is None:
            return ""
        return str(value).strip()

    item: Dict[str, Any] = {}
    st_size_value = _get("StSize")
    a_size_value = _get("ASize")
    area_value = _get("Area")

    for source_field, target_field in FIELD_MAPPING.items():
        value = _get(source_field)
        if value == "":
            continue
        if target_field == "area":
            continue
        if target_field in {"sp", "cost"}:
            numeric_value = db.parse_numeric(value)
            if numeric_value is not None:
                item[target_field] = numeric_value
            continue
        item[target_field] = value

    computed_area = db.calculate_area(st_size_value, area_value, a_size_value)
    if computed_area is not None:
        item["area"] = computed_area

    return item
