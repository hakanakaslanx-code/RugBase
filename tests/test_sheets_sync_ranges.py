import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest

from core import sheets_client, sheets_sync


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("items", "items"),
        (" items ", "items"),
        ("'Inventory'", "Inventory"),
        ('"Stock"', "Stock"),
    ],
)
def test_require_worksheet_title_strips_wrapping_quotes(raw, expected):
    assert sheets_sync.require_worksheet_title(raw) == expected


@pytest.mark.parametrize(
    "title, expected",
    [
        ("items", "'items'"),
        ("Inventory", "'Inventory'"),
        ("Sheet Name", "'Sheet Name'"),
        ("Bob's Rugs", "'Bob''s Rugs'"),
        ("'Inventory'", "'Inventory'"),
    ],
)
def test_quote_title_always_wraps_in_single_quotes(title, expected):
    assert sheets_sync.quote_worksheet_title(title) == expected


def test_a1_range_uses_single_quoted_titles():
    assert sheets_sync._a1_range("items", "A2:Z") == "'items'!A2:Z"


def test_full_column_range_and_sheet_range_format():
    assert sheets_sync.FULL_COLUMN_RANGE.startswith("A1:")
    assert sheets_sync._sheet_range(2) == "A2:AI2"
    with pytest.raises(sheets_sync.SheetsSyncError):
        sheets_sync._sheet_range(0)


def test_inventory_range_helpers_respect_title_and_columns():
    assert sheets_sync.inventory_full_range("items") == "'items'!A1:AI"
    assert sheets_sync.inventory_row_range("items", 5) == "'items'!A5:AI5"
    assert sheets_sync.inventory_column_range("items") == "'items'!A:AI"


def test_inventory_range_helpers_normalise_wrapped_quotes():
    assert sheets_sync.inventory_full_range("'Inventory'") == "'Inventory'!A1:AI"
    assert sheets_sync.inventory_row_range('"Inventory"', 3) == "'Inventory'!A3:AI3"


def test_sheets_client_range_uses_dynamic_last_column():
    columns = len(sheets_sync.HEADERS)
    assert sheets_client._range_for_title("items", columns=columns) == "'items'!A:AI"
    assert sheets_client._range_for_title("items", columns=80) == "'items'!A:CB"
