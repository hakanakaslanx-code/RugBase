"""SQLite-backed data access layer for RugBase."""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from core import app_paths
from core.sheets_client import SheetsClientError
from settings import load_google_sync_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database path handling
# ---------------------------------------------------------------------------
_DEFAULT_DB_PATH = Path(
    os.environ.get("RUGBASE_DB_PATH", str(app_paths.data_path("rugbase.db")))
).resolve()
_DB_PATH = _DEFAULT_DB_PATH
DB_PATH = _DB_PATH

_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False

# ---------------------------------------------------------------------------
# Inventory metadata shared with other modules
# ---------------------------------------------------------------------------
MASTER_SHEET_COLUMNS: List[Tuple[str, str]] = [
    ("rug_no", "RugNo"),
    ("upc", "UPC"),
    ("roll_no", "RollNo"),
    ("v_rug_no", "VtgNo"),
    ("v_collection", "VCollection"),
    ("collection", "Collection"),
    ("v_design", "VDesign"),
    ("design", "Design"),
    ("brand_name", "Brand"),
    ("ground", "Ground"),
    ("border", "Border"),
    ("a_size", "ASize"),
    ("st_size", "SSize"),
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

SHEET_HEADER_TO_COLUMN: Dict[str, str] = {
    "RugNo": "rug_no",
    "UPC": "upc",
    "RollNo": "roll_no",
    "VtgNo": "v_rug_no",
    "VCollection": "v_collection",
    "Collection": "collection",
    "VDesign": "v_design",
    "Design": "design",
    "Brand": "brand_name",
    "Ground": "ground",
    "Border": "border",
    "ASize": "a_size",
    "SSize": "st_size",
    "Area": "area",
    "Type": "type",
    "Rate": "rate",
    "Amount": "amount",
    "Shape": "shape",
    "Style": "style",
    "ImageFileName": "image_file_name",
    "Origin": "origin",
    "Retail": "retail",
    "SP": "sp",
    "MSRP": "msrp",
    "Cost": "cost",
    "price": "retail",
    "sku": "upc",
    "size": "a_size",
    "Qty": "qty",
    "Status": "status",
    "Consignment": "consignment",
    "Notes": "notes",
    "UpdatedAt": "updated_at",
    "RowID": "item_id",
    "Hash": "hash",
    "Deleted": "deleted",
}

ITEM_FIELD_ALIASES: Dict[str, str] = {
    "id": "item_id",
    "ItemId": "item_id",
    "RowID": "item_id",
    "RugNo": "rug_no",
    "UPC": "upc",
    "RollNo": "roll_no",
    "VtgNo": "v_rug_no",
    "VCollection": "v_collection",
    "Collection": "collection",
    "VDesign": "v_design",
    "Design": "design",
    "Brand": "brand_name",
    "Ground": "ground",
    "Border": "border",
    "ASize": "a_size",
    "SSize": "st_size",
    "Area": "area",
    "Type": "type",
    "Rate": "rate",
    "Amount": "amount",
    "Shape": "shape",
    "Style": "style",
    "ImageFileName": "image_file_name",
    "Origin": "origin",
    "Retail": "retail",
    "SP": "sp",
    "MSRP": "msrp",
    "Cost": "cost",
    "price": "retail",
    "sku": "upc",
    "size": "a_size",
    "Qty": "qty",
    "Status": "status",
    "Consignment": "consignment",
    "Notes": "notes",
    "UpdatedAt": "updated_at",
    "UpdatedBy": "updated_by",
    "Hash": "hash",
    "Deleted": "deleted",
    "ConsignmentId": "consignment_id",
    "SoldAt": "sold_at",
    "SalePrice": "sale_price",
    "SaleNote": "sale_note",
    "CustomerId": "customer_id",
}

ITEM_COLUMN_DEFINITIONS: Dict[str, str] = {
    "item_id": "TEXT PRIMARY KEY",
    "rug_no": "TEXT",
    "upc": "TEXT",
    "roll_no": "TEXT",
    "v_rug_no": "TEXT",
    "v_collection": "TEXT",
    "collection": "TEXT",
    "v_design": "TEXT",
    "design": "TEXT",
    "brand_name": "TEXT",
    "ground": "TEXT",
    "border": "TEXT",
    "a_size": "TEXT",
    "st_size": "TEXT",
    "area": "REAL",
    "type": "TEXT",
    "rate": "REAL",
    "amount": "REAL",
    "shape": "TEXT",
    "style": "TEXT",
    "image_file_name": "TEXT",
    "origin": "TEXT",
    "retail": "REAL",
    "sp": "REAL",
    "msrp": "REAL",
    "cost": "REAL",
    "qty": "INTEGER NOT NULL DEFAULT 0",
    "status": "TEXT NOT NULL DEFAULT 'active'",
    "location": "TEXT DEFAULT 'warehouse'",
    "consignment": "TEXT",
    "notes": "TEXT",
    "updated_at": "TEXT",
    "updated_by": "TEXT",
    "hash": "TEXT",
    "deleted": "INTEGER NOT NULL DEFAULT 0",
    "consignment_id": "TEXT",
    "sold_at": "TEXT",
    "customer_id": "TEXT",
    "sale_price": "REAL",
    "sale_note": "TEXT",
    "created_at": "TEXT",
    "version": "INTEGER NOT NULL DEFAULT 1",
}

CUSTOMER_COLUMN_DEFINITIONS: Dict[str, str] = {
    "id": "TEXT PRIMARY KEY",
    "full_name": "TEXT NOT NULL",
    "phone": "TEXT",
    "email": "TEXT",
    "address": "TEXT",
    "city": "TEXT",
    "state": "TEXT",
    "zip": "TEXT",
    "notes": "TEXT",
    "created_at": "TEXT",
    "updated_at": "TEXT",
}

CONFLICT_COLUMN_DEFINITIONS: Dict[str, str] = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "source": "TEXT NOT NULL",
    "entity_id": "TEXT",
    "message": "TEXT NOT NULL",
    "payload": "TEXT",
    "resolved": "INTEGER NOT NULL DEFAULT 0",
    "created_at": "TEXT NOT NULL",
}

FLOAT_FIELDS = {"area", "rate", "amount", "retail", "sp", "msrp", "cost", "sale_price"}
INT_FIELDS = {"qty", "version"}
BOOL_FIELDS = {"deleted"}

_ITEM_UPSERT_LISTENERS: List[Callable[[str], None]] = []
_LISTENER_LOCK = threading.Lock()

_ONLINE = False
_LAST_ERROR: Optional[str] = None
# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def set_database_path(path: Path) -> None:
    """Override the SQLite file used for storage."""

    global _DB_PATH, DB_PATH, _SCHEMA_READY
    _DB_PATH = Path(path).resolve()
    DB_PATH = _DB_PATH
    _SCHEMA_READY = False


def _ensure_schema(conn: sqlite3.Connection) -> None:
    item_columns = ",\n        ".join(
        f"{column} {definition}" for column, definition in ITEM_COLUMN_DEFINITIONS.items()
    )
    conn.execute(f"CREATE TABLE IF NOT EXISTS item (\n        {item_columns}\n    )")

    existing = {row[1] for row in conn.execute("PRAGMA table_info(item)")}
    for column, definition in ITEM_COLUMN_DEFINITIONS.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE item ADD COLUMN {column} {definition}")

    customer_columns = ",\n        ".join(
        f"{column} {definition}" for column, definition in CUSTOMER_COLUMN_DEFINITIONS.items()
    )
    conn.execute(f"CREATE TABLE IF NOT EXISTS customers (\n        {customer_columns}\n    )")

    conflict_columns = ",\n        ".join(
        f"{column} {definition}" for column, definition in CONFLICT_COLUMN_DEFINITIONS.items()
    )
    conn.execute(f"CREATE TABLE IF NOT EXISTS sync_conflicts (\n        {conflict_columns}\n    )")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_item_rug_no ON item(rug_no)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_item_upc ON item(upc)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_item_updated_at ON item(updated_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_conflict_resolved ON sync_conflicts(resolved)")


def _ensure_database() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        if _DB_PATH.parent:
            _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(_DB_PATH)
        try:
            _ensure_schema(conn)
            conn.commit()
        finally:
            conn.close()
        _SCHEMA_READY = True


def get_connection() -> sqlite3.Connection:
    _ensure_database()
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def transaction() -> Iterable[sqlite3.Connection]:
    conn = get_connection()
    try:
        conn.execute("BEGIN")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _default_updated_by() -> str:
    for env_var in ("RUGBASE_USER", "USERNAME", "USER"):
        value = os.getenv(env_var)
        if value:
            return value
    return "operator"


def _normalize_timestamp(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------

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


def calculate_area(st_size: Optional[str], area_value: Optional[str], a_size: Optional[str]) -> Optional[float]:
    if area_value:
        parsed = _clean_numeric(area_value)
        if parsed is not None:
            return parsed
    for candidate in (st_size, a_size):
        if not candidate:
            continue
        parts = [part.strip() for part in str(candidate).lower().split("x") if part.strip()]
        if len(parts) != 2:
            continue
        width = _clean_numeric(parts[0].replace(",", "."))
        height = _clean_numeric(parts[1].replace(",", "."))
        if width is not None and height is not None:
            return round(width * height, 2)
    return None


def _coerce_float(value: Any) -> Optional[float]:
    return _clean_numeric(value)


def _coerce_int(value: Any, *, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y"}:
            return True
        if lowered in {"0", "false", "no", "n"}:
            return False
    return bool(value)
# ---------------------------------------------------------------------------
# Item normalisation
# ---------------------------------------------------------------------------

def _prepare_item_payload(
    item_data: Mapping[str, Any],
    *,
    item_id: str,
    existing: Optional[sqlite3.Row],
    override_version: Optional[int] = None,
    override_updated_at: Optional[str] = None,
) -> Dict[str, Any]:
    record: Dict[str, Any] = dict(existing) if existing else {}
    for key, value in item_data.items():
        column = ITEM_FIELD_ALIASES.get(key, key)
        if column in ITEM_COLUMN_DEFINITIONS:
            record[column] = value

    record["item_id"] = item_id
    record["qty"] = _coerce_int(record.get("qty"), default=0)
    record["status"] = (record.get("status") or "active").strip() or "active"
    record["location"] = (record.get("location") or "warehouse").strip() or "warehouse"
    record["notes"] = record.get("notes") or ""
    record["consignment"] = record.get("consignment") or record.get("consignment_id") or ""
    record["sale_note"] = record.get("sale_note") or ""
    record["customer_id"] = record.get("customer_id") or ""
    record["hash"] = record.get("hash") or ""
    record["updated_by"] = record.get("updated_by") or _default_updated_by()
    record["deleted"] = 1 if _coerce_bool(record.get("deleted")) else 0

    for field in FLOAT_FIELDS:
        if field in record:
            record[field] = _coerce_float(record.get(field))

    updated_at = override_updated_at or record.get("updated_at")
    record["updated_at"] = _normalize_timestamp(updated_at) or _utc_now_iso()

    sold_at = record.get("sold_at")
    record["sold_at"] = _normalize_timestamp(sold_at) if sold_at else None

    created_at = record.get("created_at")
    record["created_at"] = created_at or _utc_now_iso()
    if override_version is not None:
        record["version"] = max(int(override_version), 1)
    else:
        current_version = _coerce_int(record.get("version"), default=1)
        if existing:
            current_version = max(_coerce_int(record.get("version"), default=1) + 1, current_version)
        record["version"] = max(current_version, 1)

    payload: Dict[str, Any] = {column: record.get(column) for column in ITEM_COLUMN_DEFINITIONS}
    return payload


def _insert_item_row(conn: sqlite3.Connection, payload: Mapping[str, Any]) -> None:
    columns = list(payload.keys())
    placeholders = ", ".join(["?" for _ in columns])
    sql = f"INSERT INTO item ({', '.join(columns)}) VALUES ({placeholders})"
    conn.execute(sql, [payload[column] for column in columns])


def _update_item_row(conn: sqlite3.Connection, item_id: str, payload: Mapping[str, Any]) -> None:
    assignments = ", ".join([f"{column} = ?" for column in payload.keys() if column != "item_id"])
    values = [payload[column] for column in payload.keys() if column != "item_id"]
    values.append(item_id)
    sql = f"UPDATE item SET {assignments} WHERE item_id = ?"
    conn.execute(sql, values)


def _row_to_item_dict(row: sqlite3.Row) -> Dict[str, Any]:
    record = dict(row)
    record["deleted"] = bool(record.get("deleted"))
    record["qty"] = _coerce_int(record.get("qty"), default=0)
    return record


def _item_row_to_sync_payload(row: Mapping[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "id": row.get("item_id", ""),
        "version": _coerce_int(row.get("version"), default=1),
        "updated_at": row.get("updated_at") or "",
        "updated_by": row.get("updated_by") or "",
    }
    for header, column in SHEET_HEADER_TO_COLUMN.items():
        value = row.get(column)
        if header == "Qty":
            payload[header] = _coerce_int(value, default=0)
        elif header == "Deleted":
            payload[header] = "TRUE" if _coerce_bool(value) else ""
        elif value is None:
            payload[header] = ""
        else:
            payload[header] = value
    return payload


# ---------------------------------------------------------------------------
# Listener management
# ---------------------------------------------------------------------------

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
# ---------------------------------------------------------------------------
# Public operations
# ---------------------------------------------------------------------------

def initialize_database() -> List[str]:
    _ensure_database()
    try:
        from consignment_repo import migrate
    except Exception:
        logger.debug("Consignment migrations unavailable", exc_info=True)
    else:
        try:
            migrate()
        except Exception:
            logger.exception("Consignment migrations failed", exc_info=True)
    return ensure_inventory_columns()


def ensure_inventory_columns() -> List[str]:
    missing: List[str] = []
    for _field, header in MASTER_SHEET_COLUMNS:
        column = SHEET_HEADER_TO_COLUMN.get(header)
        if column and column not in ITEM_COLUMN_DEFINITIONS:
            missing.append(header)
    return missing


def fetch_items(*, status_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    with get_connection() as conn:
        sql = "SELECT * FROM item"
        params: List[Any] = []
        if status_filter:
            sql += " WHERE status = ?"
            params.append(status_filter)
        sql += " ORDER BY created_at"
        cursor = conn.execute(sql, params)
        return [_row_to_item_dict(row) for row in cursor.fetchall()]


def fetch_item(item_id: str) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM item WHERE item_id = ?", (item_id,))
        row = cursor.fetchone()
        return _row_to_item_dict(row) if row else None


def upsert_item(item_data: Mapping[str, Any]) -> Tuple[str, bool]:
    item_id = str(
        item_data.get("item_id")
        or item_data.get("RowID")
        or item_data.get("id")
        or uuid.uuid4()
    )

    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM item WHERE item_id = ?", (item_id,))
        existing = cursor.fetchone()
        payload = _prepare_item_payload(
            item_data,
            item_id=item_id,
            existing=existing,
        )
        if existing:
            _update_item_row(conn, item_id, payload)
            created = False
        else:
            _insert_item_row(conn, payload)
            created = True
    _notify_item_upsert(item_id)
    return item_id, created


def delete_item(item_id: str) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM item WHERE item_id = ?", (item_id,))


def fetch_distinct_values(field: str) -> List[str]:
    if field not in ITEM_COLUMN_DEFINITIONS:
        return []
    with get_connection() as conn:
        cursor = conn.execute(
            f"SELECT DISTINCT {field} FROM item WHERE {field} IS NOT NULL AND TRIM({field}) != ''"
        )
        values = [row[0] for row in cursor.fetchall() if row[0] is not None]
    return sorted(set(str(value) for value in values))


def fetch_customers(search: Optional[str] = None) -> List[Dict[str, Any]]:
    with get_connection() as conn:
        sql = "SELECT * FROM customers"
        params: List[Any] = []
        if search:
            like = f"%{search.strip()}%"
            sql += " WHERE full_name LIKE ? OR phone LIKE ? OR email LIKE ?"
            params.extend([like, like, like])
        sql += " ORDER BY LOWER(full_name)"
        cursor = conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]


def fetch_customer(customer_id: str) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM customers WHERE id = ?", (customer_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def create_customer(customer_data: Mapping[str, Any]) -> str:
    customer_id = str(customer_data.get("id") or uuid.uuid4())
    now = _utc_now_iso()
    payload = {
        "id": customer_id,
        "full_name": customer_data.get("full_name", "").strip(),
        "phone": customer_data.get("phone", ""),
        "email": customer_data.get("email", ""),
        "address": customer_data.get("address", ""),
        "city": customer_data.get("city", ""),
        "state": customer_data.get("state", ""),
        "zip": customer_data.get("zip", ""),
        "notes": customer_data.get("notes", ""),
        "created_at": now,
        "updated_at": now,
    }
    with get_connection() as conn:
        columns = list(payload.keys())
        placeholders = ", ".join(["?" for _ in columns])
        conn.execute(
            f"INSERT INTO customers ({', '.join(columns)}) VALUES ({placeholders})",
            [payload[column] for column in columns],
        )
    return customer_id


def update_customer(customer_id: str, customer_data: Mapping[str, Any]) -> None:
    now = _utc_now_iso()
    fields = [
        "full_name",
        "phone",
        "email",
        "address",
        "city",
        "state",
        "zip",
        "notes",
    ]
    assignments = ", ".join([f"{field} = ?" for field in fields])
    values = [customer_data.get(field, "") for field in fields]
    values.append(now)
    values.append(customer_id)
    with get_connection() as conn:
        conn.execute(
            f"UPDATE customers SET {assignments}, updated_at = ? WHERE id = ?",
            values,
        )


def mark_item_sold(
    item_id: str,
    *,
    sold_at: Optional[str] = None,
    sale_price: Optional[str] = None,
    customer_id: Optional[str] = None,
    note: Optional[str] = None,
) -> None:
    payload: Dict[str, Any] = {
        "item_id": item_id,
        "status": "sold",
        "sold_at": sold_at or _utc_now_iso(),
        "sale_price": sale_price,
        "customer_id": customer_id or "",
        "sale_note": note or "",
    }
    upsert_item(payload)


def get_sales_summary(days: Optional[int] = None) -> Dict[str, float]:  # noqa: D401 - compatibility
    with get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*), COALESCE(SUM(area), 0) FROM item")
        total_items, total_area = cursor.fetchone()
    return {
        "total_items": float(total_items or 0),
        "total_area": round(float(total_area or 0.0), 2),
    }


def fetch_items_for_sync_snapshot() -> List[Dict[str, Any]]:
    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM item ORDER BY created_at")
        return [_item_row_to_sync_payload(dict(row)) for row in cursor.fetchall()]


def fetch_item_for_sync(item_id: str) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM item WHERE item_id = ?", (item_id,))
        row = cursor.fetchone()
        return _item_row_to_sync_payload(dict(row)) if row else None


def apply_remote_sync_row(row: Mapping[str, Any]) -> None:
    item_id = str(
        row.get("id")
        or row.get("item_id")
        or row.get("RowID")
        or uuid.uuid4()
    )
    override_version = _coerce_int(row.get("version"), default=1)
    override_updated_at = row.get("updated_at") or row.get("UpdatedAt")
    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM item WHERE item_id = ?", (item_id,))
        existing = cursor.fetchone()
        payload = _prepare_item_payload(
            row,
            item_id=item_id,
            existing=existing,
            override_version=override_version,
            override_updated_at=override_updated_at,
        )
        if existing:
            _update_item_row(conn, item_id, payload)
        else:
            _insert_item_row(conn, payload)
    _notify_item_upsert(item_id)


def bump_item_version(item_id: str) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE item SET version = version + 1, updated_at = ? WHERE item_id = ?",
            (_utc_now_iso(), item_id),
        )


def get_max_item_updated_at() -> Optional[str]:
    with get_connection() as conn:
        cursor = conn.execute("SELECT MAX(updated_at) FROM item")
        value = cursor.fetchone()[0]
        return str(value) if value else None


def fetch_customers_for_sheet() -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM customers ORDER BY LOWER(full_name)")
        for row in cursor.fetchall():
            record = dict(row)
            rows.append(
                {
                    "Id": record.get("id", ""),
                    "FullName": record.get("full_name", ""),
                    "Phone": record.get("phone", ""),
                    "Email": record.get("email", ""),
                    "Address": record.get("address", ""),
                    "City": record.get("city", ""),
                    "State": record.get("state", ""),
                    "Zip": record.get("zip", ""),
                    "Notes": record.get("notes", ""),
                    "CreatedAt": record.get("created_at", ""),
                    "UpdatedAt": record.get("updated_at", ""),
                }
            )
    return rows


def log_conflict(source: str, entity_id: Optional[str], message: str, payload: Mapping[str, Any]) -> None:
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO sync_conflicts (source, entity_id, message, payload, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                source,
                entity_id,
                message,
                str(payload),
                _utc_now_iso(),
            ),
        )


def count_conflicts(*, resolved: Optional[bool] = None) -> int:
    with get_connection() as conn:
        if resolved is None:
            cursor = conn.execute("SELECT COUNT(*) FROM sync_conflicts")
            return int(cursor.fetchone()[0] or 0)
        flag = 1 if resolved else 0
        cursor = conn.execute("SELECT COUNT(*) FROM sync_conflicts WHERE resolved = ?", (flag,))
        return int(cursor.fetchone()[0] or 0)
# ---------------------------------------------------------------------------
# Google Sheets synchronisation helpers
# ---------------------------------------------------------------------------

def refresh_from_remote() -> bool:
    global _ONLINE, _LAST_ERROR
    settings = load_google_sync_settings()
    if not settings.spreadsheet_id:
        _ONLINE = False
        _LAST_ERROR = "Spreadsheet ID is not configured"
        raise SheetsClientError("Spreadsheet ID is not configured")

    try:
        from core.sync_service import SyncService, SyncServiceError

        sync_service = SyncService()
        stats = sync_service.pull(settings)
    except SheetsClientError as exc:
        _ONLINE = False
        _LAST_ERROR = str(exc)
        raise
    except SyncServiceError as exc:
        _ONLINE = False
        _LAST_ERROR = str(exc)
        raise SheetsClientError(str(exc)) from exc
    except Exception as exc:
        _ONLINE = False
        _LAST_ERROR = str(exc)
        raise SheetsClientError(str(exc)) from exc

    _ONLINE = True
    _LAST_ERROR = None
    return bool(stats.get("inserted") or stats.get("updated"))


def is_online() -> bool:
    return _ONLINE


def last_sync_error() -> Optional[str]:
    return _LAST_ERROR


# ---------------------------------------------------------------------------
# Module exports
# ---------------------------------------------------------------------------

__all__ = [
    "MASTER_SHEET_COLUMNS",
    "MASTER_SHEET_FIELDS",
    "NUMERIC_FIELDS",
    "UPDATABLE_FIELDS",
    "CUSTOMER_FIELDS",
    "CUSTOMER_HEADERS",
    "SHEET_HEADER_TO_COLUMN",
    "ITEM_FIELD_ALIASES",
    "ITEM_COLUMN_DEFINITIONS",
    "CUSTOMER_COLUMN_DEFINITIONS",
    "CONFLICT_COLUMN_DEFINITIONS",
    "FLOAT_FIELDS",
    "INT_FIELDS",
    "BOOL_FIELDS",
    "DB_PATH",
    "set_database_path",
    "get_connection",
    "transaction",
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
    "add_item_upsert_listener",
    "remove_item_upsert_listener",
    "refresh_from_remote",
    "is_online",
    "last_sync_error",
    "fetch_items_for_sync_snapshot",
    "fetch_item_for_sync",
    "apply_remote_sync_row",
    "bump_item_version",
    "fetch_customers_for_sheet",
    "log_conflict",
    "count_conflicts",
    "get_max_item_updated_at",
]


def generate_item_id() -> str:
    return str(uuid.uuid4())
