from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core import google_sync


def _sample_mapping() -> google_sync.SheetMapping:
    return google_sync.SheetMapping.from_dict({"rug_no": "RugNo", "status": "Status"})


def _sample_row(index: int, *, version: int = 1, last_pushed: int = 0) -> google_sync.RowSnapshot:
    return google_sync.RowSnapshot(
        rb_id=f"RB-{index}",
        values={"rug_no": f"R-{index}", "status": "Available"},
        version=version,
        last_pushed_version=last_pushed,
        updated_at="2024-01-01T00:00:00Z",
        updated_by="tester",
    )


def test_column_letter_sequence() -> None:
    assert google_sync.column_letter(1) == "A"
    assert google_sync.column_letter(26) == "Z"
    assert google_sync.column_letter(27) == "AA"
    assert google_sync.column_letter(52) == "AZ"
    with pytest.raises(ValueError):
        google_sync.column_letter(0)


def test_format_cell_handles_various_types() -> None:
    assert google_sync.format_cell(None, google_sync.ColumnType.TEXT) == ""
    assert google_sync.format_cell(12.3456, google_sync.ColumnType.NUMBER) == "12.35"
    assert google_sync.format_cell(7.9, google_sync.ColumnType.INTEGER) == "8"
    dt = datetime(2024, 1, 1, 12, 30, tzinfo=timezone.utc)
    assert google_sync.format_cell(dt, google_sync.ColumnType.DATETIME) == "2024-01-01 12:30:00"


def test_chunk_rows_respects_max_cells() -> None:
    rows = [[f"row-{index}", "A", "B"] for index in range(10)]
    chunks = google_sync.chunk_rows(rows, max_cells=6, column_count=3)
    assert len(chunks) == 5
    assert all(len(chunk) <= 2 for chunk in chunks)


def test_sheet_synchroniser_split_batches() -> None:
    mapping = _sample_mapping()
    synchroniser = google_sync.SheetSynchroniser(mapping)
    rows = [_sample_row(index) for index in range(5)]
    batches = synchroniser.split_batches(rows, max_cells=9)
    assert len(batches) == 2
    total_rows = sum(len(batch) for batch in batches)
    assert total_rows == len(rows)
    assert all(all(len(row) == len(mapping.headers()) for row in batch) for batch in batches)


def test_detect_conflicts_reports_field_differences() -> None:
    mapping = _sample_mapping()
    synchroniser = google_sync.SheetSynchroniser(mapping)
    local = [_sample_row(1, version=3, last_pushed=1)]
    remote_versions = {"RB-1": 2}
    remote_values = {"RB-1": {"RugNo": "R-1", "Status": "Sold"}}

    conflicts = synchroniser.detect_conflicts(local, remote_versions, remote_values)

    assert len(conflicts) == 1
    conflict = conflicts[0]
    assert conflict.rb_id == "RB-1"
    assert conflict.field_diffs == {"Status": ("Available", "Sold")}


def test_serialise_sync_payload_structure() -> None:
    mapping = _sample_mapping()
    rows = [_sample_row(1, version=2, last_pushed=1)]

    payload_json = google_sync.serialise_sync_payload(rows, mapping)
    payload = json.loads(payload_json)

    assert payload["headers"] == mapping.headers()
    assert payload["rows"][0]["rb_id"] == "RB-1"
    assert payload["rows"][0]["values"] == {"rug_no": "R-1", "status": "Available"}
    assert payload["rows"][0]["version"] == 2


def test_backoff_controller_schedule_limits_growth() -> None:
    controller = google_sync.BackoffController(base=1.0, maximum=4.0, attempts=5)
    delays = controller.schedule()

    assert delays == [1.0, 2.0, 4.0, 4.0, 4.0]
    assert len(delays) == 5
