"""Google Sheets backed data access layer for RugBase."""

from __future__ import annotations

import logging
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from core.sheets_client import (
    SheetTabData,
    SheetsApiResponseError,
    SheetsClientError,
    SheetsCredentialsError,
    build_client,
)
from settings import GoogleSyncSettings, load_google_sync_settings

logger = logging.getLogger(__name__)


MASTER_SHEET_COLUMNS: List[Tuple[str, str]] = [
    ("rug_no", "RugNo"),
    ("upc", "UPC"),
    ("roll_no", "RollNo"),
    ("v_rug_no", "VRugNo"),
    ("v_collection", "Vcollection"),
    ("collection", "Collection"),
    ("v_design", "VDesign"),
    ("design", "Design"),
    ("brand_name", "Brandname"),
    ("ground", "Ground"),
    ("border", "Border"),
    ("a_size", "ASize"),
    ("st_size", "StSize"),
    ("area", "Area"),
    ("type", "Type"),
    ("rate", "Rate"),
    ("amount", "Amount"),
    ("shape", "Shape"),
    ("style", "Style"),
    ("image_file_name", "ImageFileName"),
    ("origin", "Origin"),
    ("retail", "Retail"),
    ("sp", "SP"),
    ("msrp", "MSRP"),
    ("cost", "Cost"),
]

MASTER_SHEET_FIELDS: Tuple[str, ...] = tuple(field for field, _ in MASTER_SHEET_COLUMNS)

NUMERIC_FIELDS = {"area", "sp", "cost", "rate", "amount", "retail", "msrp"}

MODULE_FIELD_COLUMN_MAP: Dict[str, Tuple[str, str]] = {
    "RugNo": ("rug_no", "TEXT"),
    "MSRP": ("msrp", "TEXT"),
    "Color": ("ground", "TEXT"),
    "Content": ("content", "TEXT"),
    "Origin": ("origin", "TEXT"),
    "Style": ("style", "TEXT"),
    "Type": ("type", "TEXT"),
}

MODULE_COLUMN_DEFINITIONS: Dict[str, str] = {
    column: definition for column, definition in MODULE_FIELD_COLUMN_MAP.values()
}

ADDITIONAL_ITEM_FIELDS: Tuple[str, ...] = tuple(
    column for column in MODULE_COLUMN_DEFINITIONS if column not in MASTER_SHEET_FIELDS
)

COLUMN_DISPLAY_NAMES: Dict[str, str] = {}
for module_field, (column, _definition) in MODULE_FIELD_COLUMN_MAP.items():
    COLUMN_DISPLAY_NAMES.setdefault(column, module_field)

INVENTORY_CORE_FIELDS: Tuple[str, ...] = MASTER_SHEET_FIELDS + ADDITIONAL_ITEM_FIELDS

UPDATABLE_FIELDS: Tuple[str, ...] = (
    *INVENTORY_CORE_FIELDS,
    "qty",
    "status",
    "location",
    "consignment_id",
    "sold_at",
    "customer_id",
    "sale_price",
    "sale_note",
)

CUSTOMER_FIELDS: Tuple[str, ...] = (
    "full_name",
    "phone",
    "email",
    "address",
    "city",
    "state",
    "zip",
    "notes",
)

CUSTOMER_HEADERS: Tuple[str, ...] = (
    "Id",
    "FullName",
    "Phone",
    "Email",
    "Address",
    "City",
    "State",
    "Zip",
    "Notes",
    "CreatedAt",
    "UpdatedAt",
)

_ITEM_UPSERT_LISTENERS: List[Callable[[str], None]] = []
_LISTENER_LOCK = threading.Lock()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _default_updated_by() -> str:
    for env_var in ("RUGBASE_USER", "USERNAME", "USER"):
        value = os.getenv(env_var)
        if value:
            return value
    return "operator"


def _normalize_timestamp(value: Optional[str]) -> str:
    if not value:
        return _utc_now().isoformat()
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return _utc_now().isoformat()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.replace(microsecond=0).isoformat()


def _clean_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    import re

    text = text.replace("\xa0", " ")
    text = re.sub(r"[^0-9.,-]", "", text)
    if not text:
        return None
    comma = text.count(",")
    dot = text.count(".")
    decimal = "."
    thousands = ""
    if comma and dot:
        decimal = "," if text.rfind(",") > text.rfind(".") else "."
        thousands = "." if decimal == "," else ","
    elif comma and not dot:
        decimal = ","
    if thousands:
        text = text.replace(thousands, "")
    if decimal == ",":
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def parse_numeric(value: Optional[str]) -> Optional[float]:
    return _clean_numeric(value)


def _normalize_dimension(value: Optional[str]) -> Optional[Tuple[float, float]]:
    if not value:
        return None
    parts = [part.strip() for part in str(value).split("x") if part.strip()]
    if len(parts) != 2:
        return None
    width = _clean_numeric(parts[0].replace(",", "."))
    height = _clean_numeric(parts[1].replace(",", "."))
    if width is None or height is None:
        return None
    return width, height


def calculate_area(st_size: Optional[str], area_value: Optional[str], a_size: Optional[str]) -> Optional[float]:
    if area_value:
        parsed = _clean_numeric(area_value)
        if parsed is not None:
            return parsed
    for candidate in (st_size, a_size):
        dims = _normalize_dimension(candidate)
        if dims:
            width, height = dims
            return round(width * height, 2)
    return None


def _notify_item_upsert(item_id: str) -> None:
    with _LISTENER_LOCK:
        listeners = list(_ITEM_UPSERT_LISTENERS)
    for listener in listeners:
        try:
            listener(item_id)
        except Exception:
            logger.exception("Item upsert listener raised an exception")


def add_item_upsert_listener(listener: Callable[[str], None]) -> None:
    if not callable(listener):
        raise TypeError("listener must be callable")
    with _LISTENER_LOCK:
        if listener not in _ITEM_UPSERT_LISTENERS:
            _ITEM_UPSERT_LISTENERS.append(listener)


def remove_item_upsert_listener(listener: Callable[[str], None]) -> None:
    with _LISTENER_LOCK:
        if listener in _ITEM_UPSERT_LISTENERS:
            _ITEM_UPSERT_LISTENERS.remove(listener)


@dataclass
class SheetSnapshot:
    headers: List[str]
    rows: List[List[str]]
    digest: str


@dataclass
class InventoryRecord:
    data: Dict[str, Any]
    index: int


class SheetsDataStore:
    """In-memory cache backed by Google Sheets."""

    def __init__(self) -> None:
        self._settings: Optional[GoogleSyncSettings] = None
        self._client = None
        self._lock = threading.RLock()
        self._inventory_headers: List[str] = []
        self._inventory_order: List[str] = []
        self._inventory: Dict[str, InventoryRecord] = {}
        self._customers: Dict[str, Dict[str, Any]] = {}
        self._logs: List[Dict[str, Any]] = []
        self._settings_rows: Dict[str, Any] = {}
        self._snapshots: Dict[str, SheetSnapshot] = {}
        self._online = False
        self._last_error: Optional[str] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def initialize(self) -> List[str]:
        settings = load_google_sync_settings()
        credential_path = Path(settings.credential_path)
        if not credential_path.exists():
            raise SheetsCredentialsError(
                f"Credentials not found: {credential_path}"
            )

        client = build_client(settings.spreadsheet_id, credential_path)
        titles = [
            settings.inventory_tab,
            settings.customers_tab,
            settings.logs_tab,
            settings.settings_tab,
        ]
        snapshots = client.fetch_tabs(titles, columns=80)
        missing_columns = self._load_inventory(settings, snapshots.get(settings.inventory_tab))
        self._load_customers(settings, snapshots.get(settings.customers_tab))
        self._load_logs(snapshots.get(settings.logs_tab))
        self._load_settings(snapshots.get(settings.settings_tab))
        with self._lock:
            self._settings = settings
            self._client = client
            self._online = True
            self._last_error = None
            self._snapshots = {
                title: SheetSnapshot(headers=data.headers, rows=data.rows, digest=self._calc_digest(data.rows))
                for title, data in snapshots.items()
            }
        return missing_columns

    def _calc_digest(self, rows: Sequence[Sequence[str]]) -> str:
        import hashlib
        import json

        payload = json.dumps(list(rows), sort_keys=True).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    # ------------------------------------------------------------------
    # Inventory helpers
    # ------------------------------------------------------------------
    def _load_inventory(self, settings: GoogleSyncSettings, tab: Optional[SheetTabData]) -> List[str]:
        headers = list(tab.headers if tab else [])
        if "ItemId" not in headers:
            headers.insert(0, "ItemId")
        if "UpdatedAt" not in headers:
            headers.append("UpdatedAt")
        if "UpdatedBy" not in headers:
            headers.append("UpdatedBy")
        header_map = {header: index for index, header in enumerate(headers)}
        missing_headers: List[str] = [header for _field, header in MASTER_SHEET_COLUMNS if header not in header_map]

        inventory: Dict[str, InventoryRecord] = {}
        order: List[str] = []
        rows = tab.rows if tab else []
        for row_index, row in enumerate(rows):
            values = list(row) + [""] * (len(headers) - len(row))
            item_id = values[header_map.get("ItemId", 0)] if header_map.get("ItemId") is not None else ""
            if not item_id:
                fallback = header_map.get("RowID")
                if fallback is not None:
                    item_id = values[fallback]
            if not item_id:
                fallback = header_map.get("RugNo")
                if fallback is not None:
                    item_id = values[fallback]
            if not item_id:
                item_id = str(uuid.uuid4())

            record: Dict[str, Any] = {"item_id": item_id}
            for field, header in MASTER_SHEET_COLUMNS:
                index = header_map.get(header)
                value = values[index] if index is not None else ""
                if field in NUMERIC_FIELDS:
                    record[field] = _clean_numeric(value)
                else:
                    record[field] = value

            record.setdefault("qty", _clean_numeric(values[header_map.get("Qty", -1)]) or 0)
            record["status"] = values[header_map.get("Status", -1)] if header_map.get("Status") is not None else "active"
            record["location"] = values[header_map.get("Location", -1)] if header_map.get("Location") is not None else "warehouse"
            record["consignment_id"] = values[header_map.get("ConsignmentId", -1)] if header_map.get("ConsignmentId") is not None else ""
            record["sold_at"] = values[header_map.get("SoldAt", -1)] if header_map.get("SoldAt") is not None else ""
            record["customer_id"] = values[header_map.get("CustomerId", -1)] if header_map.get("CustomerId") is not None else ""
            record["sale_price"] = _clean_numeric(values[header_map.get("SalePrice", -1)]) if header_map.get("SalePrice") is not None else None
            record["sale_note"] = values[header_map.get("SaleNote", -1)] if header_map.get("SaleNote") is not None else ""
            record["updated_at"] = _normalize_timestamp(values[header_map.get("UpdatedAt", -1)])
            record["updated_by"] = values[header_map.get("UpdatedBy", -1)] if header_map.get("UpdatedBy") is not None else _default_updated_by()

            inventory[item_id] = InventoryRecord(data=record, index=row_index)
            order.append(item_id)

        with self._lock:
            self._inventory_headers = headers
            self._inventory = inventory
            self._inventory_order = order
        return missing_headers

    def _write_inventory(self) -> None:
        settings = self._settings
        client = self._client
        if not settings or not client:
            raise RuntimeError("Sheets datastore not initialised")
        rows = [self._row_from_record(self._inventory[item_id].data) for item_id in self._inventory_order]
        tab = SheetTabData(title=settings.inventory_tab, headers=self._inventory_headers, rows=rows)
        try:
            client.update_tabs({settings.inventory_tab: tab})
        except SheetsClientError as exc:
            with self._lock:
                self._online = False
                self._last_error = str(exc)
            raise
        with self._lock:
            self._online = True
            self._last_error = None
            self._snapshots[settings.inventory_tab] = SheetSnapshot(
                headers=self._inventory_headers,
                rows=rows,
                digest=self._calc_digest(rows),
            )

    def _row_from_record(self, record: Mapping[str, Any]) -> List[str]:
        row: List[str] = []
        for header in self._inventory_headers:
            field = self._field_for_header(header)
            value = record.get(field)
            if field in NUMERIC_FIELDS and value is not None:
                row.append(str(value))
            elif value is None:
                row.append("")
            else:
                row.append(str(value))
        return row

    def _field_for_header(self, header: str) -> str:
        header_map = {
            "ItemId": "item_id",
            "RowID": "item_id",
            "UpdatedAt": "updated_at",
            "UpdatedBy": "updated_by",
            "Qty": "qty",
            "Status": "status",
            "Location": "location",
            "ConsignmentId": "consignment_id",
            "SoldAt": "sold_at",
            "CustomerId": "customer_id",
            "SalePrice": "sale_price",
            "SaleNote": "sale_note",
        }
        if header in header_map:
            return header_map[header]
        for field, sheet_header in MASTER_SHEET_COLUMNS:
            if sheet_header == header:
                return field
        return header.lower()

    # ------------------------------------------------------------------
    # Customer helpers
    # ------------------------------------------------------------------
    def _load_customers(self, settings: GoogleSyncSettings, tab: Optional[SheetTabData]) -> None:
        rows = tab.rows if tab else []
        customers: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            values = list(row) + [""] * (len(CUSTOMER_HEADERS) - len(row))
            data = {
                "id": values[0] or str(uuid.uuid4()),
                "full_name": values[1],
                "phone": values[2],
                "email": values[3],
                "address": values[4],
                "city": values[5],
                "state": values[6],
                "zip": values[7],
                "notes": values[8],
                "created_at": values[9] or _utc_now().isoformat(),
                "updated_at": values[10] or _utc_now().isoformat(),
            }
            customers[data["id"]] = data
        with self._lock:
            self._customers = customers

    def _write_customers(self) -> None:
        settings = self._settings
        client = self._client
        if not settings or not client:
            raise RuntimeError("Sheets datastore not initialised")
        rows = []
        for customer in self._customers.values():
            rows.append([
                customer.get("id", ""),
                customer.get("full_name", ""),
                customer.get("phone", ""),
                customer.get("email", ""),
                customer.get("address", ""),
                customer.get("city", ""),
                customer.get("state", ""),
                customer.get("zip", ""),
                customer.get("notes", ""),
                customer.get("created_at", ""),
                customer.get("updated_at", ""),
            ])
        tab = SheetTabData(title=settings.customers_tab, headers=list(CUSTOMER_HEADERS), rows=rows)
        try:
            client.update_tabs({settings.customers_tab: tab})
        except SheetsClientError as exc:
            with self._lock:
                self._online = False
                self._last_error = str(exc)
            raise
        with self._lock:
            self._online = True
            self._last_error = None
            self._snapshots[settings.customers_tab] = SheetSnapshot(
                headers=list(CUSTOMER_HEADERS),
                rows=rows,
                digest=self._calc_digest(rows),
            )

    # ------------------------------------------------------------------
    # Logs & settings
    # ------------------------------------------------------------------
    def _load_logs(self, tab: Optional[SheetTabData]) -> None:
        rows = tab.rows if tab else []
        logs: List[Dict[str, Any]] = []
        for row in rows:
            if not row:
                continue
            timestamp = row[0] if len(row) > 0 else _utc_now().isoformat()
            message = row[1] if len(row) > 1 else ""
            logs.append({"timestamp": timestamp, "message": message})
        with self._lock:
            self._logs = logs

    def _load_settings(self, tab: Optional[SheetTabData]) -> None:
        rows = tab.rows if tab else []
        settings_rows: Dict[str, Any] = {}
        for row in rows:
            if len(row) < 2:
                continue
            settings_rows[row[0]] = row[1]
        with self._lock:
            self._settings_rows = settings_rows

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
    def is_online(self) -> bool:
        with self._lock:
            return self._online

    def last_error(self) -> Optional[str]:
        with self._lock:
            return self._last_error

    def list_items(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(self._inventory[item_id].data) for item_id in self._inventory_order]

    def get_item(self, item_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            record = self._inventory.get(item_id)
            return dict(record.data) if record else None

    def upsert_item(self, payload: Mapping[str, Any]) -> Tuple[str, bool]:
        if not self.is_online():
            raise SheetsApiResponseError("Offline mode: changes cannot be saved")
        item_id = str(payload.get("item_id") or uuid.uuid4())
        created = False
        with self._lock:
            record = self._inventory.get(item_id)
            if record is None:
                created = True
                record = InventoryRecord(data={"item_id": item_id}, index=len(self._inventory_order))
                self._inventory[item_id] = record
                self._inventory_order.append(item_id)
            record.data.update(payload)
            record.data["item_id"] = item_id
            record.data["updated_at"] = _utc_now().isoformat()
            record.data["updated_by"] = _default_updated_by()
            for field in NUMERIC_FIELDS:
                record.data[field] = _clean_numeric(record.data.get(field))
        self._write_inventory()
        _notify_item_upsert(item_id)
        return item_id, created

    def delete_item(self, item_id: str) -> None:
        if not self.is_online():
            raise SheetsApiResponseError("Offline mode: changes cannot be saved")
        with self._lock:
            if item_id not in self._inventory:
                return
            self._inventory.pop(item_id)
            self._inventory_order = [value for value in self._inventory_order if value != item_id]
        self._write_inventory()

    def refresh(self) -> bool:
        settings = self._settings
        client = self._client
        if not settings or not client:
            raise RuntimeError("Sheets datastore not initialised")
        titles = [settings.inventory_tab, settings.customers_tab]
        try:
            snapshots = client.fetch_tabs(titles, columns=80)
        except SheetsClientError as exc:
            with self._lock:
                self._online = False
                self._last_error = str(exc)
            raise
        changed = False
        inventory_tab = snapshots.get(settings.inventory_tab)
        if inventory_tab:
            digest = self._calc_digest(inventory_tab.rows)
            current = self._snapshots.get(settings.inventory_tab)
            if not current or digest != current.digest:
                self._load_inventory(settings, inventory_tab)
                changed = True
        customer_tab = snapshots.get(settings.customers_tab)
        if customer_tab:
            digest = self._calc_digest(customer_tab.rows)
            current = self._snapshots.get(settings.customers_tab)
            if not current or digest != current.digest:
                self._load_customers(settings, customer_tab)
        with self._lock:
            self._online = True
            self._last_error = None
        return changed

    def list_customers(self, query: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            customers = list(self._customers.values())
        if query:
            lowered = query.lower()
            customers = [customer for customer in customers if lowered in customer.get("full_name", "").lower()]
        return [dict(customer) for customer in customers]

    def get_customer(self, customer_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            customer = self._customers.get(str(customer_id))
            return dict(customer) if customer else None

    def upsert_customer(self, data: Mapping[str, Any]) -> str:
        if not self.is_online():
            raise SheetsApiResponseError("Offline mode: changes cannot be saved")
        customer_id = str(data.get("id") or uuid.uuid4())
        with self._lock:
            record = self._customers.get(customer_id, {})
            created_at = record.get("created_at", _utc_now().isoformat())
            payload = dict(record)
            payload.update(data)
            payload["id"] = customer_id
            payload["created_at"] = created_at
            payload["updated_at"] = _utc_now().isoformat()
            self._customers[customer_id] = payload
        self._write_customers()
        return customer_id

    def append_log(self, message: str) -> None:
        timestamp = _utc_now().isoformat()
        with self._lock:
            self._logs.append({"timestamp": timestamp, "message": message})

    def mark_item_sold(
        self,
        item_id: str,
        *,
        sold_at: Optional[str] = None,
        sale_price: Optional[str] = None,
        customer_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> None:
        payload: Dict[str, Any] = {
            "sold_at": sold_at or _utc_now().isoformat(),
            "sale_price": sale_price,
            "customer_id": customer_id or "",
            "sale_note": note or "",
            "status": "sold",
        }
        self.upsert_item({"item_id": item_id, **payload})

    def sales_summary(self) -> Dict[str, float]:
        total_items = 0
        total_area = 0.0
        for record in self.list_items():
            total_items += 1
            area = record.get("area")
            if isinstance(area, (int, float)):
                total_area += float(area)
        return {"total_items": float(total_items), "total_area": round(total_area, 2)}


_DATASTORE = SheetsDataStore()


def initialize_database() -> List[str]:
    try:
        return _DATASTORE.initialize()
    except SheetsClientError as exc:
        logger.error("Google Sheets initialise failed: %s", exc)
        raise


def ensure_inventory_columns() -> List[str]:
    with _DATASTORE._lock:  # type: ignore[attr-defined]
        headers = list(_DATASTORE._inventory_headers)
    expected = [header for _field, header in MASTER_SHEET_COLUMNS]
    return [header for header in expected if header not in headers]


def fetch_items(*, status_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    items = _DATASTORE.list_items()
    if status_filter:
        return [item for item in items if item.get("status") == status_filter]
    return items


def fetch_item(item_id: str) -> Optional[Dict[str, Any]]:
    return _DATASTORE.get_item(item_id)


def upsert_item(item_data: Mapping[str, Any]) -> Tuple[str, bool]:
    return _DATASTORE.upsert_item(item_data)


def delete_item(item_id: str) -> None:
    _DATASTORE.delete_item(item_id)


def fetch_distinct_values(field: str) -> List[str]:
    values: List[str] = []
    for item in fetch_items():
        value = item.get(field)
        if value and value not in values:
            values.append(value)
    return values


def refresh_from_remote() -> bool:
    return _DATASTORE.refresh()


def is_online() -> bool:
    return _DATASTORE.is_online()


def last_sync_error() -> Optional[str]:
    return _DATASTORE.last_error()


def fetch_customers(search: Optional[str] = None) -> List[Dict[str, Any]]:
    return _DATASTORE.list_customers(search)


def fetch_customer(customer_id: str) -> Optional[Dict[str, Any]]:
    return _DATASTORE.get_customer(customer_id)


def create_customer(customer_data: Mapping[str, Any]) -> str:
    return _DATASTORE.upsert_customer(customer_data)


def update_customer(customer_id: str, customer_data: Mapping[str, Any]) -> None:
    payload = dict(customer_data)
    payload["id"] = customer_id
    _DATASTORE.upsert_customer(payload)


def mark_item_sold(
    item_id: str,
    *,
    sold_at: Optional[str] = None,
    sale_price: Optional[str] = None,
    customer_id: Optional[str] = None,
    note: Optional[str] = None,
) -> None:
    _DATASTORE.mark_item_sold(item_id, sold_at=sold_at, sale_price=sale_price, customer_id=customer_id, note=note)


def get_sales_summary(days: Optional[int] = None) -> Dict[str, float]:  # days retained for compatibility
    return _DATASTORE.sales_summary()


def generate_item_id() -> str:
    return str(uuid.uuid4())


__all__ = [
    "MASTER_SHEET_COLUMNS",
    "MASTER_SHEET_FIELDS",
    "NUMERIC_FIELDS",
    "UPDATABLE_FIELDS",
    "add_item_upsert_listener",
    "remove_item_upsert_listener",
    "initialize_database",
    "ensure_inventory_columns",
    "fetch_items",
    "fetch_item",
    "upsert_item",
    "delete_item",
    "fetch_distinct_values",
    "fetch_customers",
    "fetch_customer",
    "create_customer",
    "update_customer",
    "mark_item_sold",
    "get_sales_summary",
    "parse_numeric",
    "calculate_area",
    "generate_item_id",
]
