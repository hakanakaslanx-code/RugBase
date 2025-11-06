import os
import sqlite3
from typing import Any, Dict, List, Optional

DB_FILENAME = "inventory.db"
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_FILENAME)


CREATE_ITEM_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS item (
    item_id TEXT PRIMARY KEY,
    rug_no TEXT,
    sku TEXT,
    collection TEXT,
    brand TEXT,
    design TEXT,
    ground TEXT,
    border TEXT,
    size_label TEXT,
    area REAL,
    stock_location TEXT,
    status TEXT,
    notes TEXT,
    price_list REAL
)
"""

SAMPLE_ITEMS = [
    {
        "item_id": "ITEM-001",
        "rug_no": "RUG-1001",
        "sku": "SKU-1001",
        "collection": "Heritage",
        "brand": "RugMasters",
        "design": "Floral",
        "ground": "Blue",
        "border": "Cream",
        "size_label": "5x8",
        "area": 40.0,
        "stock_location": "Warehouse A",
        "status": "In Stock",
        "notes": "Origin: Turkey\nContent: 100% Wool",
        "price_list": 2479.0,
    },
    {
        "item_id": "ITEM-002",
        "rug_no": "RUG-1002",
        "sku": "SKU-1002",
        "collection": "Modern",
        "brand": "UrbanRugs",
        "design": "Geometric",
        "ground": "Gray",
        "border": "Black",
        "size_label": "6x9",
        "area": 54.0,
        "stock_location": "Warehouse B",
        "status": "Reserved",
        "notes": "Origin: India\nContent: Wool & Viscose",
        "price_list": 1899.5,
    },
    {
        "item_id": "ITEM-003",
        "rug_no": "RUG-1003",
        "sku": "SKU-1003",
        "collection": "Classic",
        "brand": "RugMasters",
        "design": "Medallion",
        "ground": "Red",
        "border": "Gold",
        "size_label": "8x10",
        "area": 80.0,
        "stock_location": "Showroom",
        "status": "Sold",
        "notes": "Origin: Pakistan\nContent: Hand-Spun Wool",
        "price_list": 3125.75,
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
        _ensure_additional_columns(conn)
        cursor = conn.execute("SELECT COUNT(*) FROM item")
        count = cursor.fetchone()[0]
        if count == 0:
            insert_item_sql = (
                "INSERT INTO item (item_id, rug_no, sku, collection, brand, design, ground, border, "
                "size_label, area, stock_location, status, notes, price_list) VALUES "
                "(:item_id, :rug_no, :sku, :collection, :brand, :design, :ground, :border, :size_label, :area, "
                ":stock_location, :status, :notes, :price_list)"
            )
            conn.executemany(insert_item_sql, SAMPLE_ITEMS)
            conn.commit()


def _ensure_additional_columns(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(item)")
    existing_columns = {row[1] for row in cursor.fetchall()}
    required_columns = {
        "notes": "TEXT",
        "price_list": "REAL",
    }
    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            conn.execute(f"ALTER TABLE item ADD COLUMN {column_name} {column_type}")


def fetch_items(
    collection_filter: Optional[str] = None,
    brand_filter: Optional[str] = None,
    status_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    query = (
        "SELECT item_id, rug_no, sku, collection, brand, design, ground, border, size_label, area, "
        "stock_location, status, notes, price_list FROM item"
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
            "SELECT item_id, rug_no, sku, collection, brand, design, ground, border, size_label, area, "
            "stock_location, status, notes, price_list FROM item WHERE item_id = ?",
            (item_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None


def update_item(item_data: Dict[str, Any]) -> None:
    fields = [
        "rug_no",
        "sku",
        "collection",
        "brand",
        "design",
        "ground",
        "border",
        "size_label",
        "area",
        "stock_location",
        "status",
        "notes",
        "price_list",
    ]
    set_clause = ", ".join(f"{field} = ?" for field in fields)
    params = [item_data.get(field) for field in fields]
    params.append(item_data["item_id"])

    with get_connection() as conn:
        conn.execute(
            f"UPDATE item SET {set_clause} WHERE item_id = ?",
            params,
        )
        conn.commit()


def fetch_items_by_ids(item_ids: List[str]) -> List[Dict[str, Any]]:
    if not item_ids:
        return []

    placeholders = ",".join("?" for _ in item_ids)
    query = (
        "SELECT item_id, rug_no, sku, collection, brand, design, ground, border, size_label, area, "
        "stock_location, status, notes, price_list FROM item WHERE item_id IN ("
        + placeholders
        + ") ORDER BY rug_no"
    )

    with get_connection() as conn:
        cursor = conn.execute(query, item_ids)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
