import json
import logging
import os
import re
import sqlite3
import sys
import threading
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from core import app_paths

DB_FILENAME = "rugbase.db"


def _get_base_directory() -> str:
    """Return the base directory for packaged application resources."""

    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def resource_path(*parts: str) -> str:
    """Resolve a path relative to the application base directory."""

    return os.path.join(_get_base_directory(), *parts)


def data_path(*parts: str) -> str:
    """Resolve a path inside the mutable application data directory."""

    return str(app_paths.data_path(*parts))


DB_PATH = data_path(DB_FILENAME)

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S"


def _now_iso() -> str:
    """Return the current UTC timestamp formatted as ISO 8601 without microseconds."""

    return datetime.utcnow().replace(microsecond=0).isoformat()


logger = logging.getLogger(__name__)


_ITEM_UPSERT_LISTENERS: List[Callable[[str], None]] = []
_ITEM_UPSERT_LISTENERS_LOCK = threading.Lock()


def add_item_upsert_listener(listener: Callable[[str], None]) -> None:
    """Register a callable that will be notified when an item is inserted or updated."""

    if not callable(listener):
        raise TypeError("listener must be callable")

    with _ITEM_UPSERT_LISTENERS_LOCK:
        if listener in _ITEM_UPSERT_LISTENERS:
            logger.debug("Listener %r already registered for item upsert notifications", listener)
            return
        _ITEM_UPSERT_LISTENERS.append(listener)
        logger.debug("Registered item upsert listener %r", listener)


def remove_item_upsert_listener(listener: Callable[[str], None]) -> None:
    """Remove a previously registered upsert listener."""

    with _ITEM_UPSERT_LISTENERS_LOCK:
        try:
            _ITEM_UPSERT_LISTENERS.remove(listener)
        except ValueError:
            logger.debug("Attempted to remove unknown item upsert listener %r", listener)
        else:
            logger.debug("Removed item upsert listener %r", listener)


def _notify_item_upsert(item_id: str) -> None:
    """Invoke registered listeners for an item upsert event."""

    with _ITEM_UPSERT_LISTENERS_LOCK:
        listeners = list(_ITEM_UPSERT_LISTENERS)

    if not listeners:
        logger.debug("No item upsert listeners registered; skipping notification for %s", item_id)
        return

    logger.debug(
        "Dispatching item upsert notification for %s to %d listener(s)",
        item_id,
        len(listeners),
    )

    for listener in listeners:
        try:
            listener(item_id)
        except Exception:  # pragma: no cover - defensive logging
            logger.exception("Item upsert listener %r raised an exception", listener)


PROCESSED_CHANGES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS processed_changes (
    change_file TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
)
"""


CONFLICTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS conflicts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    change_file TEXT NOT NULL,
    item_id TEXT,
    reason TEXT NOT NULL,
    payload TEXT,
    created_at TEXT NOT NULL,
    resolved INTEGER NOT NULL DEFAULT 0
)
"""


PROCESSED_STOCK_TXN_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS processed_stock_txns (
    txn_id TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
)
"""


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

NUMERIC_FIELDS = {"area", "sp", "cost"}

TABLE_COLUMNS: List[Tuple[str, str]] = [
    ("item_id", "TEXT PRIMARY KEY"),
    *[
        (field, "REAL" if field in NUMERIC_FIELDS else "TEXT")
        for field in MASTER_SHEET_FIELDS
    ],
    ("qty", "INTEGER DEFAULT 0"),
    ("created_at", "TEXT"),
    ("updated_at", "TEXT"),
    ("version", "INTEGER DEFAULT 1"),
    ("status", "TEXT DEFAULT 'in_stock'"),
    ("location", "TEXT DEFAULT 'warehouse'"),
    ("consignment_id", "INTEGER"),
]

UPDATABLE_FIELDS: Tuple[str, ...] = MASTER_SHEET_FIELDS


def _build_create_table_sql(columns: Iterable[Tuple[str, str]]) -> str:
    column_defs = ",\n".join(f"    {name} {definition}" for name, definition in columns)
    return f"CREATE TABLE IF NOT EXISTS item (\n{column_defs}\n)"


CREATE_ITEM_TABLE_SQL = _build_create_table_sql(TABLE_COLUMNS)

SAMPLE_ITEMS = [
    {
        "item_id": "ITEM-001",
        "rug_no": "RUG-1001",
        "upc": "123456789012",
        "roll_no": "RN-001",
        "v_rug_no": "VR-2001",
        "v_collection": "Vintage",
        "collection": "Heritage",
        "v_design": "Vintage Floral",
        "design": "Floral",
        "brand_name": "RugMasters",
        "ground": "Blue",
        "border": "Cream",
        "a_size": "5x8",
        "st_size": "60x96",
        "area": 40.0,
        "type": "Handmade",
        "rate": "45.00",
        "amount": "1800.00",
        "shape": "Rectangle",
        "style": "Classic",
        "image_file_name": "rug1001.jpg",
        "origin": "India",
        "retail": "2200.00",
        "sp": 2000.0,
        "msrp": "2500.00",
        "cost": 1500.0,
    },
    {
        "item_id": "ITEM-002",
        "rug_no": "RUG-1002",
        "upc": "223456789012",
        "roll_no": "RN-002",
        "v_rug_no": "VR-2002",
        "v_collection": "Modern",
        "collection": "Contemporary",
        "v_design": "Metro Geo",
        "design": "Geometric",
        "brand_name": "UrbanRugs",
        "ground": "Gray",
        "border": "Black",
        "a_size": "6x9",
        "st_size": "72x108",
        "area": 54.0,
        "type": "Machine",
        "rate": "30.00",
        "amount": "1620.00",
        "shape": "Rectangle",
        "style": "Modern",
        "image_file_name": "rug1002.jpg",
        "origin": "Turkey",
        "retail": "1900.00",
        "sp": 1700.0,
        "msrp": "2100.00",
        "cost": 1200.0,
    },
    {
        "item_id": "ITEM-003",
        "rug_no": "RUG-1003",
        "upc": "323456789012",
        "roll_no": "RN-003",
        "v_rug_no": "VR-2003",
        "v_collection": "Classic",
        "collection": "Traditions",
        "v_design": "Royal Crest",
        "design": "Medallion",
        "brand_name": "RugMasters",
        "ground": "Red",
        "border": "Gold",
        "a_size": "8x10",
        "st_size": "96x120",
        "area": 80.0,
        "type": "Handmade",
        "rate": "55.00",
        "amount": "4400.00",
        "shape": "Rectangle",
        "style": "Traditional",
        "image_file_name": "rug1003.jpg",
        "origin": "Iran",
        "retail": "5200.00",
        "sp": 5000.0,
        "msrp": "5600.00",
        "cost": 3200.0,
    },
]


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_database() -> None:
    db_directory = os.path.dirname(DB_PATH)
    if db_directory and not os.path.exists(db_directory):
        os.makedirs(db_directory, exist_ok=True)
    with get_connection() as conn:
        conn.execute(CREATE_ITEM_TABLE_SQL)
        conn.execute(PROCESSED_CHANGES_TABLE_SQL)
        conn.execute(CONFLICTS_TABLE_SQL)
        conn.execute(PROCESSED_STOCK_TXN_TABLE_SQL)
        _ensure_columns(conn)
        cursor = conn.execute("SELECT COUNT(*) FROM item")
        count = cursor.fetchone()[0]
        if count == 0:
            insert_fields = ["item_id", *MASTER_SHEET_FIELDS]
            placeholders = ", ".join(f":{field}" for field in insert_fields)
            insert_item_sql = (
                f"INSERT INTO item ({', '.join(insert_fields)}) VALUES ({placeholders})"
            )
            conn.executemany(insert_item_sql, SAMPLE_ITEMS)
            conn.commit()

    from consignment_repo import migrate

    migrate()


def fetch_items(
    rug_no_filter: Optional[str] = None,
    collection_filter: Optional[str] = None,
    brand_filter: Optional[str] = None,
    style_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    select_fields = ["item_id", *MASTER_SHEET_FIELDS, "created_at", "updated_at"]
    query = f"SELECT {', '.join(select_fields)} FROM item"
    filters = ["COALESCE(status, 'in_stock') != 'deleted'"]
    params: List[Any] = []

    if rug_no_filter:
        filters.append("LOWER(rug_no) LIKE ?")
        params.append(f"%{rug_no_filter.lower()}%")
    if collection_filter:
        filters.append("LOWER(collection) LIKE ?")
        params.append(f"%{collection_filter.lower()}%")
    if brand_filter:
        filters.append("LOWER(brand_name) LIKE ?")
        params.append(f"%{brand_filter.lower()}%")
    if style_filter:
        filters.append("LOWER(style) LIKE ?")
        params.append(f"%{style_filter.lower()}%")

    if filters:
        query += " WHERE " + " AND ".join(filters)

    query += " ORDER BY rug_no"

    with get_connection() as conn:
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def search_items_for_labels(
    rug_no: Optional[str] = None,
    collection: Optional[str] = None,
    design: Optional[str] = None,
    color: Optional[str] = None,
    size: Optional[str] = None,
    origin: Optional[str] = None,
) -> List[Dict[str, Any]]:
    select_fields = ["item_id", *MASTER_SHEET_FIELDS, "created_at", "updated_at"]
    query = f"SELECT {', '.join(select_fields)} FROM item"
    filters: List[str] = ["COALESCE(status, 'in_stock') != 'deleted'"]
    params: List[Any] = []

    def _add_filter(field: str, value: Optional[str]) -> None:
        if value:
            filters.append(f"LOWER({field}) LIKE ?")
            params.append(f"%{value.lower()}%")

    _add_filter("rug_no", rug_no)
    _add_filter("collection", collection)
    _add_filter("design", design)
    if color:
        filters.append("(LOWER(ground) LIKE ? OR LOWER(border) LIKE ?)")
        params.extend([f"%{color.lower()}%", f"%{color.lower()}%"])
    _add_filter("st_size", size)
    _add_filter("origin", origin)

    if filters:
        query += " WHERE " + " AND ".join(filters)

    query += " ORDER BY rug_no"

    with get_connection() as conn:
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def fetch_distinct_values(field: str) -> List[str]:
    if field not in MASTER_SHEET_FIELDS:
        raise ValueError(f"Bilinmeyen alan: {field}")

    query = (
        f"SELECT DISTINCT {field} FROM item "
        f"WHERE {field} IS NOT NULL AND TRIM({field}) != '' ORDER BY {field}"
    )

    with get_connection() as conn:
        cursor = conn.execute(query)
        values = [row[0] for row in cursor.fetchall() if row[0] is not None]
        return [str(value) for value in values]


def fetch_item(item_id: str) -> Optional[Dict[str, Any]]:
    select_fields = ["item_id", *MASTER_SHEET_FIELDS, "created_at", "updated_at"]
    query = f"SELECT {', '.join(select_fields)} FROM item WHERE item_id = ?"

    with get_connection() as conn:
        cursor = conn.execute(query, (item_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_item(item_data: Dict[str, Any]) -> None:
    set_clause = ", ".join(f"{field} = ?" for field in UPDATABLE_FIELDS)
    params = [item_data.get(field) for field in UPDATABLE_FIELDS]
    now = _now_iso()
    params.extend([now, item_data["item_id"]])

    with get_connection() as conn:
        conn.execute(
            f"UPDATE item SET {set_clause}, updated_at = ?, version = COALESCE(version, 0) + 1 "
            "WHERE item_id = ?",
            params,
        )
        conn.commit()

    _notify_item_upsert(item_data["item_id"])


def upsert_item(item_data: Dict[str, Any]) -> Tuple[str, bool]:
    """Upsert an item and return a tuple of (item_id, created)."""

    updatable_fields = list(UPDATABLE_FIELDS)

    set_clause = ", ".join(f"{field} = ?" for field in updatable_fields)

    match_values: List[Tuple[str, Any]] = []
    if item_data.get("rug_no"):
        match_values.append(("rug_no", item_data["rug_no"]))
    if item_data.get("upc"):
        match_values.append(("upc", item_data["upc"]))
    if item_data.get("roll_no"):
        match_values.append(("roll_no", item_data["roll_no"]))

    with get_connection() as conn:
        item_id: Optional[str] = None
        for field, value in match_values:
            cursor = conn.execute(
                f"SELECT item_id FROM item WHERE {field} = ? LIMIT 1",
                (value,),
            )
            row = cursor.fetchone()
            if row:
                item_id = row["item_id"]
                break

        if item_id:
            select_fields = ", ".join(updatable_fields)
            cursor = conn.execute(
                f"SELECT {select_fields} FROM item WHERE item_id = ?",
                (item_id,),
            )
            existing_row = cursor.fetchone()
            merged_data = {
                field: existing_row[field] if existing_row else None
                for field in updatable_fields
            }
            for field in updatable_fields:
                if field in item_data:
                    merged_data[field] = item_data[field]

            params = [merged_data.get(field) for field in updatable_fields]
            now = _now_iso()
            params.extend([now, item_id])
            conn.execute(
                f"UPDATE item SET {set_clause}, updated_at = ?, version = COALESCE(version, 0) + 1 "
                "WHERE item_id = ?",
                params,
            )
            conn.commit()
            _notify_item_upsert(item_id)
            return item_id, False

        new_item_id = item_data.get("item_id") or str(uuid.uuid4())
        now = _now_iso()
        insert_fields = ["item_id"] + updatable_fields + ["qty", "created_at", "updated_at", "version"]
        insert_values = [
            new_item_id,
            *[item_data.get(field) for field in updatable_fields],
            item_data.get("qty", 0),
            now,
            now,
            1,
        ]
        placeholders = ", ".join("?" for _ in insert_fields)
        conn.execute(
            f"INSERT INTO item ({', '.join(insert_fields)}) VALUES ({placeholders})",
            insert_values,
        )
        conn.commit()
        _notify_item_upsert(new_item_id)
        return new_item_id, True


def insert_item(item_data: Dict[str, Any]) -> str:
    """Insert a new item into the database and return its identifier."""

    item_id = item_data.get("item_id") or str(uuid.uuid4())
    now = _now_iso()
    insert_fields = ["item_id", *UPDATABLE_FIELDS, "qty", "created_at", "updated_at", "version"]
    insert_values = [
        item_id,
        *[item_data.get(field) for field in UPDATABLE_FIELDS],
        item_data.get("qty", 0),
        now,
        now,
        1,
    ]
    placeholders = ", ".join("?" for _ in insert_fields)

    with get_connection() as conn:
        conn.execute(
            f"INSERT INTO item ({', '.join(insert_fields)}) VALUES ({placeholders})",
            insert_values,
        )
        conn.commit()

    return item_id


def delete_item(item_id: str) -> None:
    """Remove an item from the database."""

    now = _now_iso()
    with get_connection() as conn:
        conn.execute(
            "UPDATE item SET status = 'deleted', updated_at = ?, version = COALESCE(version, 0) + 1 "
            "WHERE item_id = ?",
            (now, item_id),
        )
        conn.commit()


def _row_to_sync_payload(row: sqlite3.Row) -> Dict[str, Any]:
    """Convert a database row to the sync payload structure."""

    def _clean(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    payload: Dict[str, Any] = {
        "id": row["item_id"],
        "rug_no": _clean(row["rug_no"]) or "",
        "sku": _clean(row["upc"]) or "",
        "collection": _clean(row["collection"]) or "",
        "size": _clean(row["st_size"]) or "",
        "price": _clean(row["retail"]) or "",
        "qty": int(row["qty"]) if row["qty"] is not None else 0,
        "updated_at": row["updated_at"],
        "version": int(row["version"]) if row["version"] is not None else 1,
    }
    return payload


def fetch_item_for_sync(item_id: str) -> Optional[Dict[str, Any]]:
    """Return a single item row formatted for synchronisation."""

    query = (
        "SELECT item_id, rug_no, upc, collection, st_size, retail, qty, updated_at, version "
        "FROM item WHERE item_id = ?"
    )

    with get_connection() as conn:
        cursor = conn.execute(query, (item_id,))
        row = cursor.fetchone()
        return _row_to_sync_payload(row) if row else None


def fetch_items_for_sync_snapshot() -> List[Dict[str, Any]]:
    """Return all non-deleted items formatted for synchronisation."""

    query = (
        "SELECT item_id, rug_no, upc, collection, st_size, retail, qty, updated_at, version "
        "FROM item WHERE COALESCE(status, 'in_stock') != 'deleted'"
    )

    with get_connection() as conn:
        cursor = conn.execute(query)
        rows = cursor.fetchall()
    return [_row_to_sync_payload(row) for row in rows]


def apply_remote_sync_row(row: Dict[str, Any]) -> None:
    """Apply a row received from Google Sheets to the local database."""

    item_id = row["id"]
    now = row.get("updated_at") or _now_iso()
    version = int(row.get("version") or 1)
    qty_value = row.get("qty")
    try:
        qty = int(qty_value) if qty_value is not None else 0
    except (TypeError, ValueError):
        qty = 0

    def _normalise(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    values = (
        _normalise(row.get("rug_no")),
        _normalise(row.get("sku")),
        _normalise(row.get("collection")),
        _normalise(row.get("size")),
        _normalise(row.get("price")),
        qty,
        now,
        version,
        item_id,
    )

    insert_values = (
        item_id,
        values[0],
        values[1],
        values[2],
        values[3],
        values[4],
        qty,
        now,
        now,
        version,
    )

    with get_connection() as conn:
        cursor = conn.execute("SELECT 1 FROM item WHERE item_id = ?", (item_id,))
        if cursor.fetchone():
            conn.execute(
                """
                UPDATE item
                SET rug_no = ?, upc = ?, collection = ?, st_size = ?, retail = ?, qty = ?,
                    updated_at = ?, version = ?
                WHERE item_id = ?
                """,
                values,
            )
        else:
            conn.execute(
                """
                INSERT INTO item (
                    item_id, rug_no, upc, collection, st_size, retail, qty,
                    created_at, updated_at, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                insert_values,
            )
        conn.commit()

    _notify_item_upsert(item_id)


def bump_item_version(item_id: str) -> None:
    """Increment an item's version and touch the update timestamp."""

    now = _now_iso()
    with get_connection() as conn:
        conn.execute(
            "UPDATE item SET version = COALESCE(version, 0) + 1, updated_at = ? WHERE item_id = ?",
            (now, item_id),
        )
        conn.commit()

    _notify_item_upsert(item_id)


def generate_item_id() -> str:
    """Generate a short unique identifier for a new item."""

    return f"ITEM-{uuid.uuid4().hex[:8].upper()}"


def _parse_numeric(value: str) -> Optional[float]:
    cleaned = value.strip()
    if not cleaned:
        return None

    normalized = cleaned.replace(" ", "")
    try:
        return float(normalized.replace(",", ""))
    except ValueError:
        if "," in normalized and "." not in normalized:
            try:
                return float(normalized.replace(",", "."))
            except ValueError:
                return None
        return None


def parse_numeric(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    return _parse_numeric(str(value))


def has_processed_change(change_file: str) -> bool:
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT 1 FROM processed_changes WHERE change_file = ?",
            (change_file,),
        )
        return cursor.fetchone() is not None


def record_processed_change(change_file: str, applied_at: Optional[str] = None) -> None:
    timestamp = applied_at or datetime.utcnow().strftime(ISO_FORMAT)
    with get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO processed_changes (change_file, applied_at) VALUES (?, ?)",
            (change_file, timestamp),
        )
        conn.commit()


def has_processed_stock_txn(txn_id: str) -> bool:
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT 1 FROM processed_stock_txns WHERE txn_id = ?",
            (txn_id,),
        )
        return cursor.fetchone() is not None


def record_processed_stock_txn(txn_id: str, applied_at: Optional[str] = None) -> None:
    timestamp = applied_at or datetime.utcnow().strftime(ISO_FORMAT)
    with get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO processed_stock_txns (txn_id, applied_at) VALUES (?, ?)",
            (txn_id, timestamp),
        )
        conn.commit()


def log_conflict(
    change_file: str,
    item_id: Optional[str],
    reason: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    payload_json = None
    if payload is not None:
        try:
            payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        except TypeError:
            payload_json = json.dumps({"repr": repr(payload)})

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO conflicts (change_file, item_id, reason, payload, created_at, resolved)
            VALUES (?, ?, ?, ?, ?, 0)
            """,
            (
                change_file,
                item_id,
                reason,
                payload_json,
                datetime.utcnow().strftime(ISO_FORMAT),
            ),
        )
        conn.commit()


def fetch_conflicts(resolved: Optional[bool] = None) -> List[Dict[str, Any]]:
    query = "SELECT * FROM conflicts"
    params: List[object] = []
    if resolved is not None:
        query += " WHERE resolved = ?"
        params.append(1 if resolved else 0)
    query += " ORDER BY datetime(created_at) DESC"

    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def count_conflicts(resolved: Optional[bool] = None) -> int:
    query = "SELECT COUNT(*) FROM conflicts"
    params: List[object] = []
    if resolved is not None:
        query += " WHERE resolved = ?"
        params.append(1 if resolved else 0)

    with get_connection() as conn:
        cursor = conn.execute(query, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def resolve_conflict(conflict_id: int) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE conflicts SET resolved = 1 WHERE id = ?",
            (conflict_id,),
        )
        conn.commit()


_DIMENSION_PATTERN = re.compile(r"[0-9]+(?:\.[0-9]+)?")


def _extract_dimensions(value: Optional[str]) -> Optional[Tuple[float, float]]:
    if not value:
        return None

    cleaned = (
        value.replace("×", "x")
        .replace("X", "x")
        .replace("'", " ")
        .replace("\"", " ")
        .replace("ft", " ")
        .replace("FT", " ")
    )
    cleaned = cleaned.replace(",", ".")
    numbers = _DIMENSION_PATTERN.findall(cleaned)
    if len(numbers) < 2:
        return None

    try:
        width = float(numbers[0])
        height = float(numbers[1])
    except ValueError:
        return None

    return width, height


def calculate_area(
    st_size: Optional[str],
    area_value: Optional[str] = None,
    a_size: Optional[str] = None,
) -> Optional[float]:
    """Calculate the area from the provided values following master sheet rules."""

    for raw in (area_value,):
        if raw is None:
            continue
        parsed = _parse_numeric(str(raw))
        if parsed is not None:
            return parsed

    for dimensions_source in (st_size, a_size):
        dimensions = _extract_dimensions(dimensions_source)
        if dimensions:
            width, height = dimensions
            return round(width * height, 4)

    return None


def _ensure_columns(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(item)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    for column, definition in TABLE_COLUMNS:
        if column not in existing_columns:
            conn.execute(f"ALTER TABLE item ADD COLUMN {column} {definition}")

    # Normalise timestamps and versions for existing rows
    conn.execute("UPDATE item SET created_at = REPLACE(created_at, ' ', 'T') WHERE created_at LIKE '% %'")
    conn.execute("UPDATE item SET updated_at = REPLACE(updated_at, ' ', 'T') WHERE updated_at LIKE '% %'")

    now = _now_iso()
    conn.execute("UPDATE item SET created_at = ? WHERE created_at IS NULL OR created_at = ''", (now,))
    conn.execute("UPDATE item SET updated_at = ? WHERE updated_at IS NULL OR updated_at = ''", (now,))
    conn.execute("UPDATE item SET version = COALESCE(NULLIF(version, 0), 1)")
    conn.commit()
