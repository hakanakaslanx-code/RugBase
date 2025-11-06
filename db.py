import os
import re
import sqlite3
import sys
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

DB_FILENAME = "inventory.db"


def _get_base_directory() -> str:
    """Return the base directory for storing runtime data files."""

    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def resource_path(*parts: str) -> str:
    """Resolve a path relative to the application base directory."""

    return os.path.join(_get_base_directory(), *parts)


DB_PATH = resource_path(DB_FILENAME)


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
    ("created_at", "TEXT DEFAULT (CURRENT_TIMESTAMP)"),
    ("updated_at", "TEXT DEFAULT (CURRENT_TIMESTAMP)"),
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


def fetch_items(
    collection_filter: Optional[str] = None,
    brand_filter: Optional[str] = None,
    style_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    select_fields = ["item_id", *MASTER_SHEET_FIELDS, "created_at", "updated_at"]
    query = f"SELECT {', '.join(select_fields)} FROM item"
    filters = []
    params: List[Any] = []

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
    params.append(item_data["item_id"])

    with get_connection() as conn:
        conn.execute(
            f"UPDATE item SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE item_id = ?",
            params,
        )
        conn.commit()


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


def insert_item(item_data: Dict[str, Any]) -> str:
    """Insert a new item into the database and return its identifier."""

    item_id = item_data.get("item_id") or str(uuid.uuid4())
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    insert_fields = ["item_id", *UPDATABLE_FIELDS, "created_at", "updated_at"]
    insert_values = [
        item_id,
        *[item_data.get(field) for field in UPDATABLE_FIELDS],
        now,
        now,
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

    with get_connection() as conn:
        conn.execute("DELETE FROM item WHERE item_id = ?", (item_id,))
        conn.commit()


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

    # Ensure timestamps are populated for existing rows
    conn.execute(
        "UPDATE item SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)"
    )
    conn.execute(
        "UPDATE item SET updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)"
    )
    conn.commit()
