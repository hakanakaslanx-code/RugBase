"""Tests for automatic inventory column maintenance."""

from __future__ import annotations

from pathlib import Path
from typing import List

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import consignment_repo
import db


def _set_temp_db(tmp_path, monkeypatch) -> Path:
    temp_db = tmp_path / "rugbase.db"
    monkeypatch.setattr(db, "DB_PATH", str(temp_db))
    monkeypatch.setattr(consignment_repo, "migrate", lambda: None)
    return temp_db


def _existing_columns() -> List[str]:
    with db.get_connection() as conn:
        cursor = conn.execute("PRAGMA table_info(item)")
        return [row[1] for row in cursor.fetchall()]


def test_ensure_inventory_columns_adds_missing_fields(tmp_path, monkeypatch):
    _set_temp_db(tmp_path, monkeypatch)
    with db.get_connection() as conn:
        conn.execute("CREATE TABLE item (item_id TEXT PRIMARY KEY, rug_no TEXT)")
        conn.commit()

    added = db.ensure_inventory_columns()
    assert "content" in added
    columns = _existing_columns()
    assert "content" in columns

    added_again = db.ensure_inventory_columns()
    assert added_again == []


def test_initialize_database_reports_added_columns(tmp_path, monkeypatch):
    _set_temp_db(tmp_path, monkeypatch)
    with db.get_connection() as conn:
        conn.execute("CREATE TABLE item (item_id TEXT PRIMARY KEY, rug_no TEXT)")
        conn.commit()

    added = db.initialize_database()
    assert "content" in added

    added_after_setup = db.initialize_database()
    assert added_after_setup == []
