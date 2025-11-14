from pathlib import Path

import db


def _configure_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "inventory.db"
    if db_path.exists():
        db_path.unlink()
    db.set_database_path(db_path)
    return db_path


def test_initialize_database_creates_schema(tmp_path):
    db_path = _configure_db(tmp_path)
    missing = db.initialize_database()
    assert missing == []
    assert db_path.exists()
    with db.get_connection() as conn:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='item'"
        )
        assert cursor.fetchone() is not None


def test_ensure_inventory_columns_reports_no_missing(tmp_path):
    _configure_db(tmp_path)
    db.initialize_database()
    missing = db.ensure_inventory_columns()
    assert missing == []
