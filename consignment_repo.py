"""Database access functions for consignment workflows."""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

import sqlite3

import db

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S"


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    """Provide a SQLite connection wrapped in an explicit transaction."""

    conn = db.get_connection()
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("BEGIN")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    cursor = conn.execute(f"PRAGMA table_info({table})")
    columns = {row[1] for row in cursor.fetchall()}
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def migrate() -> None:
    """Apply consignment-related schema migrations."""

    with transaction() as conn:
        _ensure_column(conn, "item", "status", "TEXT DEFAULT 'in_stock'")
        _ensure_column(conn, "item", "location", "TEXT DEFAULT 'warehouse'")
        _ensure_column(conn, "item", "consignment_id", "INTEGER")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS consignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consignment_ref TEXT UNIQUE NOT NULL,
                partner_name TEXT NOT NULL,
                partner_contact TEXT,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                notes TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS consignment_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consignment_id INTEGER NOT NULL,
                item_id TEXT NOT NULL,
                rug_no TEXT NOT NULL,
                scanned_at TEXT NOT NULL,
                scanned_by TEXT,
                qty INTEGER NOT NULL DEFAULT 1,
                unit_price REAL,
                state TEXT NOT NULL DEFAULT 'out',
                FOREIGN KEY (consignment_id) REFERENCES consignments(id) ON DELETE CASCADE,
                FOREIGN KEY (item_id) REFERENCES item(item_id) ON DELETE CASCADE
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                occurred_at TEXT NOT NULL,
                user TEXT,
                action TEXT NOT NULL,
                payload TEXT
            )
            """
        )

        conn.execute(
            "UPDATE item SET status = COALESCE(status, 'in_stock') WHERE status IS NULL"
        )
        conn.execute(
            "UPDATE item SET location = COALESCE(location, 'warehouse') WHERE location IS NULL"
        )


def _log_event(conn: sqlite3.Connection, user: str, action: str, payload: Dict[str, Any]) -> None:
    conn.execute(
        "INSERT INTO events (occurred_at, user, action, payload) VALUES (?, ?, ?, ?)",
        (
            datetime.utcnow().strftime(ISO_FORMAT),
            user,
            action,
            json.dumps(payload, ensure_ascii=False),
        ),
    )


def generate_consignment_ref() -> str:
    return f"CONS-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"


def create_consignment(
    partner_name: str,
    partner_contact: Optional[str],
    notes: Optional[str],
    user: str,
    consignment_ref: Optional[str] = None,
) -> Dict[str, Any]:
    if not partner_name:
        raise ValueError("Partner name is required")

    ref = consignment_ref or generate_consignment_ref()
    created_at = datetime.utcnow().strftime(ISO_FORMAT)

    with transaction() as conn:
        cursor = conn.execute(
            """
            INSERT INTO consignments (
                consignment_ref, partner_name, partner_contact, created_at, status, notes
            ) VALUES (?, ?, ?, ?, 'active', ?)
            """,
            (ref, partner_name, partner_contact, created_at, notes),
        )
        consignment_id = cursor.lastrowid
        _log_event(
            conn,
            user,
            "consignment.created",
            {
                "consignment_id": consignment_id,
                "consignment_ref": ref,
                "partner_name": partner_name,
            },
        )

    return {
        "id": consignment_id,
        "consignment_ref": ref,
        "partner_name": partner_name,
        "partner_contact": partner_contact,
        "created_at": created_at,
        "status": "active",
        "notes": notes,
    }


def fetch_item_by_rug_no(rug_no: str) -> Optional[Dict[str, Any]]:
    with db.get_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM item WHERE rug_no = ?", (rug_no,))
        row = cursor.fetchone()
        return dict(row) if row else None


def fetch_active_consignments() -> List[Dict[str, Any]]:
    with db.get_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            SELECT * FROM consignments
             WHERE status IN ('active', 'sold')
             ORDER BY datetime(created_at) DESC
            """
        )
        return [dict(row) for row in cursor.fetchall()]


def fetch_all_consignments() -> List[Dict[str, Any]]:
    with db.get_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            SELECT c.*, COALESCE(SUM(CASE WHEN cl.state = 'out' THEN cl.qty ELSE 0 END), 0) AS total_out,
                   COALESCE(SUM(CASE WHEN cl.state = 'returned' THEN cl.qty ELSE 0 END), 0) AS total_returned,
                   COUNT(cl.id) AS line_count
            FROM consignments c
            LEFT JOIN consignment_lines cl ON cl.consignment_id = c.id
            GROUP BY c.id
            ORDER BY datetime(c.created_at) DESC
            """
        )
        return [dict(row) for row in cursor.fetchall()]


def fetch_consignment_lines(consignment_id: int) -> List[Dict[str, Any]]:
    with db.get_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            SELECT cl.*, i.collection, i.design, i.brand_name
            FROM consignment_lines cl
            LEFT JOIN item i ON i.item_id = cl.item_id
            WHERE cl.consignment_id = ?
            ORDER BY datetime(cl.scanned_at) DESC
            """,
            (consignment_id,),
        )
        return [dict(row) for row in cursor.fetchall()]


def fetch_partner_names() -> List[str]:
    with db.get_connection() as conn:
        cursor = conn.execute(
            "SELECT DISTINCT partner_name FROM consignments ORDER BY LOWER(partner_name)"
        )
        return [row[0] for row in cursor.fetchall() if row[0]]


def process_scan(
    rug_no: str,
    user: str,
    consignment_id: Optional[int] = None,
    new_consignment_data: Optional[Dict[str, Optional[str]]] = None,
    qty: int = 1,
    unit_price: Optional[float] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Handle an outbound scan for a rug.

    Returns a tuple of (item, consignment).
    """

    item = fetch_item_by_rug_no(rug_no)
    if not item:
        raise ValueError(f"Rug number {rug_no} not found")

    if item.get("status") and item["status"] != "in_stock":
        raise ValueError(f"Item {rug_no} is not in stock (current status: {item['status']})")

    consignment: Optional[Dict[str, Any]] = None
    if consignment_id is None:
        if not new_consignment_data:
            raise ValueError("Consignment information is required")
        consignment = create_consignment(
            partner_name=new_consignment_data.get("partner_name", ""),
            partner_contact=new_consignment_data.get("partner_contact"),
            notes=new_consignment_data.get("notes"),
            user=user,
            consignment_ref=new_consignment_data.get("consignment_ref"),
        )
        consignment_id = consignment["id"]
    else:
        with db.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM consignments WHERE id = ?",
                (consignment_id,),
            )
            row = cursor.fetchone()
            if not row:
                raise ValueError("Selected consignment not found")
            consignment = dict(row)

    scanned_at = datetime.utcnow().strftime(ISO_FORMAT)

    with transaction() as conn:
        cursor = conn.execute(
            """
            INSERT INTO consignment_lines (
                consignment_id, item_id, rug_no, scanned_at, scanned_by, qty, unit_price, state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'out')
            """,
            (
                consignment_id,
                item["item_id"],
                rug_no,
                scanned_at,
                user,
                qty,
                unit_price,
            ),
        )
        line_id = cursor.lastrowid

        conn.execute(
            """
            UPDATE item
               SET status = 'out_on_consignment',
                   location = ?,
                   consignment_id = ?,
                   updated_at = CURRENT_TIMESTAMP
             WHERE item_id = ?
            """,
            (f"consignment:{consignment_id}", consignment_id, item["item_id"]),
        )

        _log_event(
            conn,
            user,
            "consignment.scan_out",
            {
                "consignment_id": consignment_id,
                "consignment_ref": consignment["consignment_ref"],
                "consignment_line_id": line_id,
                "item_id": item["item_id"],
                "rug_no": rug_no,
                "qty": qty,
            },
        )

    return item, consignment


def process_return_scan(rug_no: str, user: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Handle a return scan for an item."""

    item = fetch_item_by_rug_no(rug_no)
    if not item:
        raise ValueError(f"Rug number {rug_no} not found")

    consignment_id = item.get("consignment_id")
    if not consignment_id:
        raise ValueError(f"Item {rug_no} is not assigned to a consignment")

    with db.get_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT * FROM consignments WHERE id = ?",
            (consignment_id,),
        )
        consignment_row = cursor.fetchone()
        if not consignment_row:
            raise ValueError("Consignment not found for the item")
        consignment = dict(consignment_row)

    with transaction() as conn:
        cursor = conn.execute(
            """
            SELECT id FROM consignment_lines
             WHERE item_id = ? AND consignment_id = ? AND state = 'out'
             ORDER BY datetime(scanned_at) DESC
             LIMIT 1
            """,
            (item["item_id"], consignment_id),
        )
        line = cursor.fetchone()
        if not line:
            raise ValueError("No outbound record found for this item")

        conn.execute(
            "UPDATE consignment_lines SET state = 'returned' WHERE id = ?",
            (line[0],),
        )

        conn.execute(
            """
            UPDATE item
               SET status = 'in_stock',
                   location = 'warehouse',
                   consignment_id = NULL,
                   updated_at = CURRENT_TIMESTAMP
             WHERE item_id = ?
            """,
            (item["item_id"],),
        )

        cursor = conn.execute(
            """
            SELECT COUNT(*) FROM consignment_lines
             WHERE consignment_id = ? AND state = 'out'
            """,
            (consignment_id,),
        )
        remaining_out = cursor.fetchone()[0]
        if remaining_out == 0:
            conn.execute(
                "UPDATE consignments SET status = 'returned' WHERE id = ?",
                (consignment_id,),
            )
            consignment["status"] = "returned"

        _log_event(
            conn,
            user,
            "consignment.return",
            {
                "consignment_id": consignment_id,
                "consignment_ref": consignment["consignment_ref"],
                "item_id": item["item_id"],
                "rug_no": rug_no,
            },
        )

    return item, consignment


def update_consignment_status(consignment_id: int, status: str, user: str) -> None:
    with transaction() as conn:
        conn.execute(
            "UPDATE consignments SET status = ? WHERE id = ?",
            (status, consignment_id),
        )
        _log_event(
            conn,
            user,
            "consignment.status_change",
            {
                "consignment_id": consignment_id,
                "status": status,
            },
        )
