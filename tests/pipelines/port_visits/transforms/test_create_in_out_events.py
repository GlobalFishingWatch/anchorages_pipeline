import datetime

import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to

from pipe_anchorages.pipelines.port_visits.transforms.create_in_out_events import (
    CreateInOutEvents,
)

from .factories import PORT_LAT, PORT_LON, at, record

END_OF_DAY = 24 * 60

AT_SEA = dict(port_dist=10.0, speed=10.0)
IN_PORT = dict(port_dist=1.0, speed=5.0)
STOPPED = dict(port_dist=1.0, speed=0.0)


def create(min_gap_minutes=60, end_time=at(END_OF_DAY)):
    return CreateInOutEvents(
        anchorage_entry_dist=3.0,
        anchorage_exit_dist=4.0,
        stopped_begin_speed=0.2,
        stopped_end_speed=0.5,
        min_gap_minutes=min_gap_minutes,
        end_time=end_time,
    )


def events_of(records, **kwargs):
    _, events = create(**kwargs).create_in_out_events(("vessel-1", records))
    return [(e.event_type, e.timestamp) for e in events]


def test_entering_stopping_and_leaving_port_yields_one_event_per_transition():
    records = [
        record(0, **AT_SEA), record(10, **IN_PORT), record(20, **STOPPED),
        record(30, **IN_PORT), record(40, **AT_SEA),
    ]

    assert events_of(records) == [
        ("PORT_ENTRY", at(10)),
        ("PORT_STOP_BEGIN", at(20)),
        ("PORT_STOP_END", at(30)),
        ("PORT_EXIT", at(40)),
    ]


def test_stopping_right_on_arrival_and_leaving_while_stopped_yields_both_events():
    records = [record(0, **AT_SEA), record(10, **STOPPED), record(20, **AT_SEA)]

    assert events_of(records) == [
        ("PORT_ENTRY", at(10)),
        ("PORT_STOP_BEGIN", at(10)),
        ("PORT_STOP_END", at(20)),
        ("PORT_EXIT", at(20)),
    ]


def test_between_entry_and_exit_distances_a_vessel_keeps_its_previous_state():
    between = dict(port_dist=3.5, speed=5.0)
    records = [
        record(0, **AT_SEA), record(10, **between),  # still at sea
        record(20, **IN_PORT), record(30, **between),  # still in port
        record(40, **AT_SEA),
    ]

    assert events_of(records) == [("PORT_ENTRY", at(20)), ("PORT_EXIT", at(40))]


def test_between_stop_and_start_speeds_a_vessel_keeps_its_previous_state():
    slow = dict(port_dist=1.0, speed=0.3)
    records = [
        record(0, **AT_SEA), record(10, **IN_PORT), record(20, **slow),  # not stopped yet
        record(30, **STOPPED), record(40, **slow),  # still stopped
        record(50, **AT_SEA),
    ]

    assert events_of(records) == [
        ("PORT_ENTRY", at(10)),
        ("PORT_STOP_BEGIN", at(30)),
        ("PORT_STOP_END", at(50)),
        ("PORT_EXIT", at(50)),
    ]


def test_records_are_processed_in_time_order():
    records = [record(10, **IN_PORT), record(20, **AT_SEA), record(0, **AT_SEA)]

    assert events_of(records) == [("PORT_ENTRY", at(10)), ("PORT_EXIT", at(20))]


def test_a_long_gap_in_port_ending_at_a_possible_gap_end_yields_gap_events():
    records = [
        record(0, **AT_SEA), record(10, **IN_PORT),
        record(130, **IN_PORT, is_possible_gap_end=True), record(140, **AT_SEA),
    ]

    assert events_of(records, min_gap_minutes=60) == [
        ("PORT_ENTRY", at(10)),
        ("PORT_GAP_END", at(130)),
        ("PORT_GAP_BEGIN", at(70)),
        ("PORT_EXIT", at(140)),
    ]


@pytest.mark.parametrize(
    "gap_end_minutes, is_possible_gap_end",
    [(130, False), (60, True)],
    ids=["not-a-possible-gap-end", "shorter-than-min-gap"],
)
def test_no_gap_events_without_a_possible_gap_end_or_a_long_enough_gap(
    gap_end_minutes, is_possible_gap_end
):
    records = [
        record(0, **AT_SEA), record(10, **IN_PORT),
        record(gap_end_minutes, **IN_PORT, is_possible_gap_end=is_possible_gap_end),
        record(gap_end_minutes + 10, **AT_SEA),
    ]

    assert [t for t, _ in events_of(records, min_gap_minutes=60)] == ["PORT_ENTRY", "PORT_EXIT"]


def test_a_vessel_still_in_port_at_the_end_of_the_range_gets_a_gap_begin():
    records = [record(0, **AT_SEA), record(10, **IN_PORT)]

    assert events_of(records, min_gap_minutes=60, end_time=at(END_OF_DAY)) == [
        ("PORT_ENTRY", at(10)),
        ("PORT_GAP_BEGIN", at(70)),
    ]


def test_no_gap_begin_when_the_last_record_is_within_min_gap_of_the_range_end():
    records = [record(END_OF_DAY - 20, **AT_SEA), record(END_OF_DAY - 10, **IN_PORT)]

    assert events_of(records, min_gap_minutes=60, end_time=at(END_OF_DAY)) == [
        ("PORT_ENTRY", at(END_OF_DAY - 10)),
    ]


@pytest.mark.parametrize("minutes_before_end, gap_begin", [(61, True), (60, False)])
def test_the_range_ends_just_before_the_exclusive_end_time(minutes_before_end, gap_begin):
    last = END_OF_DAY - minutes_before_end
    records = [record(last - 10, **AT_SEA), record(last, **IN_PORT)]

    types = [t for t, _ in events_of(records, min_gap_minutes=60, end_time=at(END_OF_DAY))]

    assert ("PORT_GAP_BEGIN" in types) is gap_begin


def test_events_are_located_at_the_last_anchorage_the_vessel_was_in_port_at():
    records = [record(0, **AT_SEA), record(10, **IN_PORT, port="a1"), record(20, **AT_SEA)]

    _, (entry, exit_) = create().create_in_out_events(("vessel-1", records))

    assert (exit_.anchorage_id, exit_.lat, exit_.lon) == ("a1", PORT_LAT, PORT_LON)
    assert exit_.vessel_lat == records[2].location.lat
    assert (exit_.ssvid, exit_.vessel_id, exit_.seg_id) == ("111", "vessel-1", "seg-1")
    assert exit_.last_timestamp == at(10)


def test_min_gap_must_be_under_one_day():
    with pytest.raises(AssertionError, match="min gap must be under one day"):
        create(min_gap_minutes=datetime.timedelta(days=1).total_seconds() / 60)


def test_transform_maps_each_vessel_to_its_events(pipeline):
    records = [record(0, **AT_SEA), record(10, **IN_PORT), record(20, **AT_SEA)]

    with pipeline as p:
        output = p | beam.Create([("vessel-1", records)]) | create()
        types = output | beam.MapTuple(lambda k, events: (k, [e.event_type for e in events]))
        assert_that(types, equal_to([("vessel-1", ["PORT_ENTRY", "PORT_EXIT"])]))
