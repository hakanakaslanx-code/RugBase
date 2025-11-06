import os
import sqlite3
import sys
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

DB_FILENAME = "inventory.db"


def _get_database_directory() -> str:
    """Return a filesystem directory suitable for storing the SQLite DB."""

    if getattr(sys, "frozen", False):  # Running inside a PyInstaller bundle
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


DB_PATH = os.path.join(_get_database_directory(), DB_FILENAME)


# Column name -> SQLite type/default clause (excluding NOT NULL/PRIMARY KEY handled separately)
TABLE_COLUMNS: Dict[str, str] = {
    "item_id": "TEXT PRIMARY KEY",
    "rug_no": "TEXT",
    "sku": "TEXT",
    "type": "TEXT",
    "collection": "TEXT",
    "brand": "TEXT",
    "v_design": "TEXT",
    "design": "TEXT",
    "ground": "TEXT",
    "border": "TEXT",
    "size_label": "TEXT",
    "st_size": "TEXT",
    "area": "REAL",
    "stock_location": "TEXT",
    "godown": "TEXT",
    "purchase_date": "TEXT",
    "pv_no": "TEXT",
    "vendor": "TEXT",
    "sold_on": "TEXT",
    "invoice_no": "TEXT",
    "customer": "TEXT",
    "status": "TEXT",
    "payment_status": "TEXT",
    "notes": "TEXT",
    "created_at": "TEXT DEFAULT (CURRENT_TIMESTAMP)",
    "updated_at": "TEXT DEFAULT (CURRENT_TIMESTAMP)",
}

CREATE_ITEM_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS item (
    item_id TEXT PRIMARY KEY,
    rug_no TEXT,
    sku TEXT,
    type TEXT,
    collection TEXT,
    brand TEXT,
    v_design TEXT,
    design TEXT,
    ground TEXT,
    border TEXT,
    size_label TEXT,
    st_size TEXT,
    area REAL,
    stock_location TEXT,
    godown TEXT,
    purchase_date TEXT,
    pv_no TEXT,
    vendor TEXT,
    sold_on TEXT,
    invoice_no TEXT,
    customer TEXT,
    status TEXT,
    payment_status TEXT,
    notes TEXT,
    created_at TEXT DEFAULT (CURRENT_TIMESTAMP),
    updated_at TEXT DEFAULT (CURRENT_TIMESTAMP)
)
"""

SAMPLE_ITEMS = [
    {
        "item_id": "ITEM-001",
        "rug_no": "RUG-1001",
        "sku": "SKU-1001",
        "type": "Handmade",
        "collection": "Heritage",
        "brand": "RugMasters",
        "v_design": "Vintage Floral",
        "design": "Floral",
        "ground": "Blue",
        "border": "Cream",
        "size_label": "5x8",
        "st_size": "60x96",
        "area": 40.0,
        "stock_location": "Warehouse A",
        "godown": "Main",
        "purchase_date": "2022-01-15",
        "pv_no": "PV-001",
        "vendor": "Heritage Suppliers",
        "sold_on": None,
        "invoice_no": None,
        "customer": None,
        "status": "In Stock",
        "payment_status": "Pending",
        "notes": "Prime display piece",
    },
    {
        "item_id": "ITEM-002",
        "rug_no": "RUG-1002",
        "sku": "SKU-1002",
        "type": "Machine",
        "collection": "Modern",
        "brand": "UrbanRugs",
        "v_design": "Metro Geo",
        "design": "Geometric",
        "ground": "Gray",
        "border": "Black",
        "size_label": "6x9",
        "st_size": "72x108",
        "area": 54.0,
        "stock_location": "Warehouse B",
        "godown": "Secondary",
        "purchase_date": "2022-06-20",
        "pv_no": "PV-002",
        "vendor": "Urban Suppliers",
        "sold_on": None,
        "invoice_no": None,
        "customer": None,
        "status": "Reserved",
        "payment_status": "Pending",
        "notes": "Reserved for client showcase",
    },
    {
        "item_id": "ITEM-003",
        "rug_no": "RUG-1003",
        "sku": "SKU-1003",
        "type": "Handmade",
        "collection": "Classic",
        "brand": "RugMasters",
        "v_design": "Royal Crest",
        "design": "Medallion",
        "ground": "Red",
        "border": "Gold",
        "size_label": "8x10",
        "st_size": "96x120",
        "area": 80.0,
        "stock_location": "Showroom",
        "godown": "Showroom",
        "purchase_date": "2021-09-10",
        "pv_no": "PV-003",
        "vendor": "Classic Imports",
        "sold_on": "2023-03-05",
        "invoice_no": "INV-3001",
        "customer": "James & Co.",
        "status": "Sold",
        "payment_status": "Paid",
        "notes": "Delivered with premium padding",
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
        _ensure_columns(conn)
        cursor = conn.execute("SELECT COUNT(*) FROM item")
        count = cursor.fetchone()[0]
        if count == 0:
            insert_item_sql = (
                "INSERT INTO item (item_id, rug_no, sku, type, collection, brand, v_design, design, ground, border, "
                "size_label, st_size, area, stock_location, godown, purchase_date, pv_no, vendor, sold_on, invoice_no, "
                "customer, status, payment_status, notes) VALUES (:item_id, :rug_no, :sku, :type, :collection, :brand, :v_design, "
                ":design, :ground, :border, :size_label, :st_size, :area, :stock_location, :godown, :purchase_date, :pv_no, :vendor, "
                ":sold_on, :invoice_no, :customer, :status, :payment_status, :notes)"
            )
            conn.executemany(insert_item_sql, SAMPLE_ITEMS)
            conn.commit()


def fetch_items(
    collection_filter: Optional[str] = None,
    brand_filter: Optional[str] = None,
    status_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    query = (
        "SELECT item_id, rug_no, sku, type, collection, brand, v_design, design, ground, border, size_label, st_size, area, "
        "stock_location, godown, purchase_date, pv_no, vendor, sold_on, invoice_no, customer, status, payment_status, notes, "
        "created_at, updated_at FROM item"
    )
    filters = []
    params: List[Any] = []

    if collection_filter:
        filters.append("LOWER(collection) LIKE ?")
        params.append(f"%{collection_filter.lower()}%")
    if brand_filter:
        filters.append("LOWER(brand) LIKE ?")
        params.append(f"%{brand_filter.lower()}%")
    if status_filter:
        filters.append("LOWER(status) LIKE ?")
        params.append(f"%{status_filter.lower()}%")

    if filters:
        query += " WHERE " + " AND ".join(filters)

    query += " ORDER BY rug_no"

    with get_connection() as conn:
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def fetch_item(item_id: str) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT item_id, rug_no, sku, type, collection, brand, v_design, design, ground, border, size_label, st_size, area, "
            "stock_location, godown, purchase_date, pv_no, vendor, sold_on, invoice_no, customer, status, payment_status, notes, "
            "created_at, updated_at FROM item WHERE item_id = ?",
            (item_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None


def update_item(item_data: Dict[str, Any]) -> None:
    fields = [
        "rug_no",
        "sku",
        "type",
        "collection",
        "brand",
        "v_design",
        "design",
        "ground",
        "border",
        "size_label",
        "st_size",
        "area",
        "stock_location",
        "godown",
        "purchase_date",
        "pv_no",
        "vendor",
        "sold_on",
        "invoice_no",
        "customer",
        "status",
        "payment_status",
        "notes",
    ]
    set_clause = ", ".join(f"{field} = ?" for field in fields)
    params = [item_data.get(field) for field in fields]
    params.append(item_data["item_id"])

    with get_connection() as conn:
        conn.execute(
            f"UPDATE item SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE item_id = ?",
            params,
        )
        conn.commit()


def upsert_item(item_data: Dict[str, Any]) -> Tuple[str, bool]:
    """Upsert an item and return a tuple of (item_id, created)."""

    updatable_fields = [
        "rug_no",
        "sku",
        "type",
        "collection",
        "brand",
        "v_design",
        "design",
        "ground",
        "border",
        "size_label",
        "st_size",
        "area",
        "stock_location",
        "godown",
        "purchase_date",
        "pv_no",
        "vendor",
        "sold_on",
        "invoice_no",
        "customer",
        "status",
        "payment_status",
        "notes",
    ]

    set_clause = ", ".join(f"{field} = ?" for field in updatable_fields)

    match_values: List[Tuple[str, Any]] = []
    if item_data.get("rug_no"):
        match_values.append(("rug_no", item_data["rug_no"]))
    if item_data.get("sku"):
        match_values.append(("sku", item_data["sku"]))

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
            params.append(item_id)
            conn.execute(
                f"UPDATE item SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE item_id = ?",
                params,
            )
            conn.commit()
            return item_id, False

        new_item_id = item_data.get("item_id") or str(uuid.uuid4())
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        insert_fields = ["item_id"] + updatable_fields + ["created_at", "updated_at"]
        insert_values = [
            new_item_id,
            *[item_data.get(field) for field in updatable_fields],
            now,
            now,
        ]
        placeholders = ", ".join("?" for _ in insert_fields)
        conn.execute(
            f"INSERT INTO item ({', '.join(insert_fields)}) VALUES ({placeholders})",
            insert_values,
        )
        conn.commit()
        return new_item_id, True


def _ensure_columns(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(item)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    for column, definition in TABLE_COLUMNS.items():
        if column not in existing_columns:
            conn.execute(f"ALTER TABLE item ADD COLUMN {column} {definition}")

    # Ensure timestamps are populated for existing rows
    conn.execute(
        "UPDATE item SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)"
    )
    conn.execute(
        "UPDATE item SET updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)"
    )
    conn.commit()
