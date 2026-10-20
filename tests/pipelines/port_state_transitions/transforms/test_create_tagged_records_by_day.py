from pipe_anchorages.pipelines.port_state_transitions.transforms.create_tagged_records_by_day import (  # noqa: E501
    CreateTaggedRecordsByDay,
)
from pipe_anchorages.records import VesselLocationRecord

from .factories import at, position

NEXT_DAY = 24 * 60


def group(records):
    """Runs CreateTaggedRecordsByDay's steps on `records` without Beam.

    Groups the records by key as its GroupByKey does, between to_shuffle and from_shuffle.
    Returns {(identifier, date): records}.
    """
    target = CreateTaggedRecordsByDay(record_type=VesselLocationRecord)
    groups = {}
    for record in records:
        key, value = target.to_shuffle(target.add_date_to_key((record.identifier, record)))
        groups.setdefault(key, []).append(value)
    items = [
        target.tag_records(
            target.dedup_by_timestamp(target.order_by_timestamp(target.from_shuffle(item)))
        )
        for item in groups.items()
    ]
    return dict(items)


def test_records_are_grouped_by_segment_and_day_and_sorted():
    records = [
        position(20, 1.0, 5.0), position(0, 1.0, 5.0),
        position(NEXT_DAY, 1.0, 5.0), position(10, 1.0, 5.0, seg_id="seg-2"),
    ]

    groups = group(records)

    assert {key: [r.timestamp for r in rcds] for key, rcds in groups.items()} == {
        ("seg-1", "2024-01-02"): [at(0), at(20)],
        ("seg-1", "2024-01-03"): [at(NEXT_DAY)],
        ("seg-2", "2024-01-02"): [at(10)],
    }


def test_records_with_the_same_timestamp_are_kept_once():
    records = [position(0, 1.0, 5.0), position(0, 2.0, 5.0), position(10, 1.0, 5.0)]

    (rcds,) = group(records).values()

    assert len(rcds) == 2


def test_positions_get_an_empty_destination():
    # Documents current behavior: port-state-transitions never produces info records (its rows
    # have destination=None, so CreateVesselRecords drops them as invalid), so every position
    # gets the empty destination.
    records = [position(0, 1.0, 5.0), position(10, 1.0, 5.0)]

    (rcds,) = group(records).values()

    assert [r.destination for r in rcds] == ["", ""]
