"""Tests for the item upsert notification helpers in :mod:`db`."""

from __future__ import annotations

from pathlib import Path
from typing import List

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db


def test_upsert_notifies_registered_listeners(tmp_path, monkeypatch):
    temp_db = tmp_path / "rugbase.db"
    monkeypatch.setattr(db, "DB_PATH", str(temp_db))
    db.initialize_database()

    received: List[str] = []

    def listener(item_id: str) -> None:
        received.append(item_id)

    db.add_item_upsert_listener(listener)
    try:
        item_id, created = db.upsert_item({"rug_no": "RUG-XYZ"})
        assert created is True
        assert received == [item_id]

        received.clear()
        updated_id, created = db.upsert_item({"rug_no": "RUG-XYZ", "design": "Updated"})
        assert created is False
        assert updated_id == item_id
        assert received == [item_id]
    finally:
        db.remove_item_upsert_listener(listener)


def test_add_item_upsert_listener_requires_callable():
    try:
        db.add_item_upsert_listener(42)  # type: ignore[arg-type]
    except TypeError:
        pass
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("Non-callable listener should raise TypeError")
