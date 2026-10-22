import datetime

import pytest

from pipe_anchorages.pipelines.port_visits.transforms.create_in_out_events import (
    CreateInOutEvents,
)
from pipe_anchorages.pipelines.port_visits.transforms.create_port_visits import CreatePortVisits

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


def test_of_records_at_the_same_timestamp_the_one_of_the_largest_segment_is_kept():
    # seg-a is the vessel's track; seg-b is a single point at 10, at sea. Walking both would
    # yield an entry and an exit at 10 and a second entry at 20.
    track = [record(0, **AT_SEA, seg_id="seg-a"), record(10, **IN_PORT, seg_id="seg-a"),
             record(20, **IN_PORT, seg_id="seg-a"), record(30, **AT_SEA, seg_id="seg-a")]
    single_point = record(10, **AT_SEA, seg_id="seg-b")
    expected = [("PORT_ENTRY", at(10)), ("PORT_EXIT", at(30))]

    assert events_of([*track, single_point]) == expected
    assert events_of([single_point, *reversed(track)]) == expected


def test_of_records_of_equally_large_segments_the_lowest_seg_id_is_kept():
    # Both single points: seg-1 wins (seg_ids compare as strings, so seg-10 would beat seg-2).
    in_port = record(10, **IN_PORT, seg_id="seg-1")
    at_sea = record(10, **AT_SEA, seg_id="seg-2")
    track = [record(0, **AT_SEA, seg_id="seg-0"), record(20, **AT_SEA, seg_id="seg-3")]
    expected = [("PORT_ENTRY", at(10)), ("PORT_EXIT", at(20))]

    assert events_of([*track, in_port, at_sea]) == expected
    assert events_of([*track, at_sea, in_port]) == expected


def test_a_burst_of_single_points_at_one_instant_yields_at_most_one_transition():
    # A feed burst: four single-point segments at 10 alternating between port and sea.
    track = [record(0, **AT_SEA, seg_id="seg-a"), record(20, **IN_PORT, seg_id="seg-a"),
             record(30, **AT_SEA, seg_id="seg-a")]
    burst = [record(10, **(IN_PORT if i % 2 else AT_SEA), seg_id=f"seg-b-{i}")
             for i in range(1, 5)]

    assert events_of([*track, *burst]) == [("PORT_ENTRY", at(10)), ("PORT_EXIT", at(30))]


def test_a_lone_single_point_segment_is_kept():
    # E.g. a vessel in port that only pings now and then: its single point is not a tie.
    records = [record(0, **AT_SEA, seg_id="seg-a"), record(10, **IN_PORT, seg_id="seg-b"),
               record(20, **AT_SEA, seg_id="seg-a")]

    assert events_of(records) == [("PORT_ENTRY", at(10)), ("PORT_EXIT", at(20))]


def test_a_gap_ending_at_a_tied_instant_still_yields_gap_events():
    track = [
        record(0, **AT_SEA, seg_id="seg-a"), record(10, **IN_PORT, seg_id="seg-a"),
        record(130, **IN_PORT, is_possible_gap_end=True, seg_id="seg-a"),
        record(140, **AT_SEA, seg_id="seg-a"),
    ]
    single_point = record(130, **AT_SEA, is_possible_gap_end=True, seg_id="seg-b")

    assert events_of([*track, single_point], min_gap_minutes=60) == [
        ("PORT_ENTRY", at(10)),
        ("PORT_GAP_END", at(130)),
        ("PORT_GAP_BEGIN", at(70)),
        ("PORT_EXIT", at(140)),
    ]


def test_a_burst_at_one_instant_yields_unique_visit_ids():
    # Before one record per instant, each entry/exit pair at 10 became a zero-duration visit
    # with the same first event, hence the same visit_id.
    track = [record(0, **AT_SEA, seg_id="seg-a"), record(20, **AT_SEA, seg_id="seg-a")]
    burst = [record(10, **(IN_PORT if i % 2 else AT_SEA), seg_id=f"seg-b-{i}")
             for i in range(1, 7)]
    _, events = create().create_in_out_events(("vessel-1", [*track, *burst]))

    visits = list(CreatePortVisits(max_interseg_dist_nm=60.0).create_port_visits(
        (("111", "vessel-1"), events)
    ))

    assert len(visits) == 1
    assert len({v.visit_id for v in visits}) == len(visits)


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
