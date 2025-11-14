from pathlib import Path

import pytest

import db


def _configure_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "notifications.db"
    if db_path.exists():
        db_path.unlink()
    db.set_database_path(db_path)
    db.initialize_database()
    return db_path


def test_upsert_notifies_registered_listeners(tmp_path):
    _configure_db(tmp_path)

    received: list[str] = []

    def listener(item_id: str) -> None:
        received.append(item_id)

    db.add_item_upsert_listener(listener)
    try:
        item_id, created = db.upsert_item({"rug_no": "RUG-XYZ", "retail": "$2.500,00"})
        assert created is True
        assert received == [item_id]

        received.clear()
        updated_id, created = db.upsert_item({"item_id": item_id, "design": "Updated"})
        assert created is False
        assert updated_id == item_id
        assert received == [item_id]

        item = db.fetch_item(item_id)
        assert item is not None
        assert pytest.approx(item.get("retail"), rel=1e-5) == 2500.0
        assert item.get("design") == "Updated"
    finally:
        db.remove_item_upsert_listener(listener)


def test_parse_numeric_handles_currency():
    assert db.parse_numeric("$2.500,00") == pytest.approx(2500.0)
    assert db.parse_numeric("86,1") == pytest.approx(86.1)
    assert db.parse_numeric(" ") is None
