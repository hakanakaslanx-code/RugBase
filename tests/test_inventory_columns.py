from __future__ import annotations

from pathlib import Path

import pytest

import db
from core.sheets_client import SheetTabData
from settings import GoogleSyncSettings


class StubClient:
    def __init__(self, inventory_headers):
        self.tabs = {
            "items": SheetTabData(title="items", headers=inventory_headers, rows=[]),
            "Customers": SheetTabData(title="Customers", headers=[], rows=[]),
            "Logs": SheetTabData(title="Logs", headers=[], rows=[]),
            "Settings": SheetTabData(title="Settings", headers=[], rows=[]),
        }

    def fetch_tabs(self, titles, *, columns=80):
        return {title: self.tabs[title] for title in titles}

    def update_tabs(self, payload):
        pass


@pytest.fixture
def stubbed_datastore(tmp_path, monkeypatch):
    credentials = tmp_path / "cred.json"
    credentials.write_text("{}", encoding="utf-8")
    settings = GoogleSyncSettings(
        spreadsheet_id="TEST",
        credential_path=str(credentials),
        worksheet_title="items",
        inventory_tab="items",
        customers_tab="Customers",
        logs_tab="Logs",
        settings_tab="Settings",
    )
    monkeypatch.setattr(db, "load_google_sync_settings", lambda: settings)
    client = StubClient(["ItemId", "RugNo"])
    monkeypatch.setattr(db, "build_client", lambda spreadsheet_id, credential_path: client)
    monkeypatch.setattr(db, "_DATASTORE", db.SheetsDataStore())
    return client


def test_ensure_inventory_columns_reports_missing(stubbed_datastore):
    db.initialize_database()
    missing = db.ensure_inventory_columns()
    assert "Retail" in missing
    assert "RugNo" not in missing
