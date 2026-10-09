import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that

from pipe_anchorages.pipelines.port_state_transitions.transforms.create_tagged_records_by_day import (  # noqa: E501
    CreateTaggedRecordsByDay,
)

from .factories import at, info, position

NEXT_DAY = 24 * 60


def run(pipeline, records, check):
    with pipeline as p:
        grouped = (
            p
            | beam.Create([(r.identifier, r) for r in records])
            | CreateTaggedRecordsByDay()
        )
        assert_that(grouped, check)


def test_records_are_grouped_by_segment_and_day_and_sorted(pipeline):
    records = [
        position(20, 1.0, 5.0), position(0, 1.0, 5.0),
        position(NEXT_DAY, 1.0, 5.0), position(10, 1.0, 5.0, seg_id="seg-2"),
    ]

    def check(groups):
        by_key = {key: [r.timestamp for r in rcds] for key, rcds in groups}
        assert by_key == {
            ("seg-1", "2024-01-02"): [at(0), at(20)],
            ("seg-1", "2024-01-03"): [at(NEXT_DAY)],
            ("seg-2", "2024-01-02"): [at(10)],
        }

    run(pipeline, records, check)


def test_records_with_the_same_timestamp_are_kept_once(pipeline):
    records = [position(0, 1.0, 5.0), position(0, 2.0, 5.0), position(10, 1.0, 5.0)]

    def check(groups):
        ((_, rcds),) = groups
        assert len(rcds) == 2

    run(pipeline, records, check)


def test_positions_get_an_empty_destination(pipeline):
    # Documents current behavior: port-state-transitions never produces info records (its rows
    # have destination=None, so CreateVesselRecords drops them as invalid), so every position
    # gets the empty destination.
    records = [position(0, 1.0, 5.0), position(10, 1.0, 5.0)]

    def check(groups):
        ((_, rcds),) = groups
        assert [r.destination for r in rcds] == ["", ""]

    run(pipeline, records, check)


def test_info_records_make_the_grouping_fail(pipeline):
    # Documents current behavior: records are sorted by (timestamp, speed, location) before the
    # destinations are tagged, and info records have neither, so tagging positions with the
    # destination of the info records before them can't happen.
    records = [position(0, 1.0, 5.0), info(5, "BUENOS AIRES"), position(10, 1.0, 5.0)]

    with pytest.raises(AttributeError, match="speed"):
        run(pipeline, records, lambda groups: None)
