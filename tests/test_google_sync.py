from __future__ import annotations

from datetime import datetime

from core.google_sync import RowSnapshot, SheetMapping, SheetSynchroniser, build_config_sheet, column_letter


def _mapping() -> SheetMapping:
    return SheetMapping.from_dict(
        {
            "sku": "SKU",
            "title": "Title",
            "price": "Price",
            "qty": "Qty",
            "updated_at": "UpdatedAt",
            "notes": "Notes",
        }
    )


def test_detect_dirty_rows_identifies_new_and_modified_entries() -> None:
    mapping = _mapping()
    synchroniser = SheetSynchroniser(mapping)
    rows = [
        RowSnapshot(rb_id="rb-1", values={"sku": "A", "price": 10}, version=1, last_pushed_version=0),
        RowSnapshot(rb_id="rb-2", values={"sku": "B", "price": 25}, version=2, last_pushed_version=2),
        RowSnapshot(rb_id="rb-3", values={"sku": "C", "price": 30}, version=4, last_pushed_version=3),
    ]

    dirty = synchroniser.detect_dirty_rows(rows)

    assert [row.rb_id for row in dirty] == ["rb-1", "rb-3"]


def test_build_row_payload_formats_values_by_type() -> None:
    mapping = _mapping()
    synchroniser = SheetSynchroniser(mapping)
    timestamp = datetime(2024, 1, 1, 12, 30, 45)
    row = RowSnapshot(
        rb_id="rb-1",
        values={
            "sku": "00123",
            "title": "Rug",
            "price": "12.5",
            "qty": "4.9",
            "updated_at": timestamp.isoformat(),
            "notes": "Leading space",
        },
        version=1,
        last_pushed_version=0,
        updated_at=timestamp.isoformat(),
        updated_by="tester",
    )

    payload = synchroniser.build_row_payload(row)

    assert payload == [
        "rb-1",
        "00123",
        "Rug",
        "12.50",
        "5",
        "2024-01-01 12:30:45",
        "Leading space",
    ]


def test_conflict_detection_reports_remote_changes() -> None:
    mapping = SheetMapping.from_dict({"price": "Price", "qty": "Qty"})
    synchroniser = SheetSynchroniser(mapping)
    local_row = RowSnapshot(
        rb_id="rb-9",
        values={"price": "19.99", "qty": 2},
        version=3,
        last_pushed_version=2,
    )

    conflicts = synchroniser.detect_conflicts(
        [local_row],
        remote_versions={"rb-9": 4},
        remote_values={"rb-9": {"Price": "21.00", "Qty": "2"}},
    )

    assert len(conflicts) == 1
    conflict = conflicts[0]
    assert conflict.rb_id == "rb-9"
    assert conflict.remote_version == 4
    assert conflict.field_diffs == {"Price": ("19.99", "21.00")}


def test_batch_split_limits_total_cells_per_request() -> None:
    mapping = SheetMapping.from_dict({"price": "Price", "qty": "Qty", "notes": "Notes"})
    synchroniser = SheetSynchroniser(mapping)
    rows = [
        RowSnapshot(
            rb_id=f"rb-{index}",
            values={"price": index * 1.25, "qty": index, "notes": f"note {index}"},
            version=1,
            last_pushed_version=0,
        )
        for index in range(5000)
    ]

    batches = synchroniser.split_batches(rows, max_cells=10_000)

    assert sum(len(batch) for batch in batches) == 5000
    assert all(len(batch) * len(synchroniser.headers()) <= 10_000 for batch in batches)


def test_config_sheet_lists_column_letters() -> None:
    mapping = SheetMapping.from_dict({"price": "Price", "qty": "Qty"})
    config_sheet = build_config_sheet(mapping)

    assert config_sheet[0] == ["Column", "Letter"]
    # rb_id column should map to A
    assert any(row == ["rb_id", column_letter(1)] for row in config_sheet[1:])
