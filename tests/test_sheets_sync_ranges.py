import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest

from core import sheets_sync


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
    assert sheets_sync._quote_title(title) == expected


def test_a1_range_uses_single_quoted_titles():
    assert sheets_sync._a1_range("items", "A2:Z") == "'items'!A2:Z"


def test_full_column_range_and_sheet_range_format():
    assert sheets_sync.FULL_COLUMN_RANGE.startswith("A1:")
    assert sheets_sync._sheet_range(2) == "A2:AI2"
    with pytest.raises(sheets_sync.SheetsSyncError):
        sheets_sync._sheet_range(0)
