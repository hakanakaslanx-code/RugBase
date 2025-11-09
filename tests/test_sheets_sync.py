from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.sheets_sync import HEADERS, SheetRow, calc_hash, chunked, detect_local_deltas


def _make_row(row_id: str, **overrides: str) -> SheetRow:
    values = {header: "" for header in HEADERS}
    values["RowID"] = row_id
    values.update({key: str(value) for key, value in overrides.items()})
    values["Hash"] = calc_hash(values)
    return SheetRow(row_id=row_id, values=values, hash=values["Hash"])


def test_calc_hash_is_deterministic_with_unordered_fields() -> None:
    base = _make_row(
        "A-1",
        RugNo="R100",
        SKU="12345",
        Price="199.99",
        Qty="2",
    )
    reordered_values = dict(base.values)
    reordered_values.update({"Qty": "2", "Price": "199.99"})
    reordered_values["Hash"] = calc_hash(reordered_values)

    assert base.hash == reordered_values["Hash"]


def test_detect_local_deltas_finds_new_and_changed_rows() -> None:
    existing = _make_row("ROW-1", Price="100", Qty="5")
    changed = _make_row("ROW-2", Price="120", Qty="3")
    new_row = _make_row("ROW-3", Price="80", Qty="1")

    previous = {existing.row_id: existing.hash, changed.row_id: calc_hash({**changed.values, "Price": "110"})}
    rows = [existing, changed, new_row]

    new_items, changed_items = detect_local_deltas(rows, previous)

    assert [row.row_id for row in new_items] == ["ROW-3"]
    assert [row.row_id for row in changed_items] == ["ROW-2"]


def test_chunked_splits_sequence_into_expected_batches() -> None:
    data = list(range(1050))
    batches = list(chunked(data, max_size=500))

    assert len(batches) == 3
    assert len(batches[0]) == 500
    assert len(batches[1]) == 500
    assert len(batches[2]) == 50
    assert all(isinstance(batch, list) for batch in batches)
