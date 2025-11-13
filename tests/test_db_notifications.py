from __future__ import annotations

from pathlib import Path
from typing import Dict

import pytest

import db
from core.sheets_client import SheetTabData
from settings import GoogleSyncSettings


class FakeSheetsClient:
    def __init__(self, tabs: Dict[str, SheetTabData]):
        self.tabs = tabs
        self.updated_payload = None

    def fetch_tabs(self, titles, *, columns=80):
        return {title: self.tabs.get(title, SheetTabData(title, [], [])) for title in titles}

    def update_tabs(self, payload):
        self.updated_payload = payload


@pytest.fixture
def google_settings(tmp_path, monkeypatch):
    credentials = tmp_path / "credentials.json"
    credentials.write_text("{}", encoding="utf-8")
    settings = GoogleSyncSettings(
        spreadsheet_id="TEST",
        credential_path=str(credentials),
        worksheet_title="items",
        inventory_tab="Inventory",
        customers_tab="Customers",
        logs_tab="Logs",
        settings_tab="Settings",
    )
    monkeypatch.setattr(db, "load_google_sync_settings", lambda: settings)
    return settings


@pytest.fixture
def fake_client(monkeypatch, google_settings):
    tabs = {
        "Inventory": SheetTabData(title="Inventory", headers=["ItemId", "RugNo", "Retail"], rows=[]),
        "Customers": SheetTabData(title="Customers", headers=[], rows=[]),
        "Logs": SheetTabData(title="Logs", headers=[], rows=[]),
        "Settings": SheetTabData(title="Settings", headers=[], rows=[]),
    }
    client = FakeSheetsClient(tabs)
    monkeypatch.setattr(db, "build_client", lambda spreadsheet_id, credential_path: client)
    monkeypatch.setattr(db, "_DATASTORE", db.SheetsDataStore())
    return client


def test_upsert_notifies_registered_listeners(fake_client, google_settings):
    db.initialize_database()

    received: list[str] = []

    def listener(item_id: str) -> None:
        received.append(item_id)

    db.add_item_upsert_listener(listener)
    try:
        item_id, created = db.upsert_item({"rug_no": "RUG-XYZ", "retail": "$2.500,00"})
        assert created is True
        assert received == [item_id]
        assert fake_client.updated_payload is not None

        received.clear()
        updated_id, created = db.upsert_item({"item_id": item_id, "design": "Updated"})
        assert created is False
        assert updated_id == item_id
        assert received == [item_id]
    finally:
        db.remove_item_upsert_listener(listener)


def test_initialize_database_parses_numeric(fake_client, google_settings):
    inventory = SheetTabData(
        title="Inventory",
        headers=["ItemId", "RugNo", "Retail", "UpdatedAt"],
        rows=[["ITEM-1", "RUG-100", "$1.234,56", "2023-01-01T10:00:00Z"]],
    )
    fake_client.tabs["Inventory"] = inventory

    db.initialize_database()
    items = db.fetch_items()
    assert items[0]["retail"] == pytest.approx(1234.56)


def test_parse_numeric_handles_currency():
    assert db.parse_numeric("$2.500,00") == pytest.approx(2500.0)
    assert db.parse_numeric("86,1") == pytest.approx(86.1)
    assert db.parse_numeric(" ") is None
