import collections
import datetime

import pytest

from pipe_anchorages.pipelines.port_state_transitions.transforms.create_tagged_anchorages import (
    CreateTaggedAnchorages,
)
from pipe_anchorages.pipelines.port_state_transitions.transforms.smart_thin_records import (
    SmartThinRecords,
)

from .factories import NAMED_ANCHORAGE, PORT_LAT, PORT_LON, at, position

AT_SEA = dict(km=10.0, speed=10.0)
IN_PORT = dict(km=1.0, speed=5.0)
STOPPED = dict(km=1.0, speed=0.0)
FAR_AWAY = dict(km=1000.0, speed=10.0)


def anchorage_map():
    """The S2 token -> anchorages map SmartThinRecords gets as a side input."""
    transform = CreateTaggedAnchorages()
    anchorages = collections.defaultdict(list)
    for token, anchorage in transform.tag_anchorage_with_s2ids(
        transform.dict_to_psuedo_anchorage(NAMED_ANCHORAGE)
    ):
        anchorages[token].append(anchorage)
    return dict(anchorages)


def thin(records, min_gap_minutes=240):
    transform = SmartThinRecords(
        anchorages=None,
        anchorage_entry_dist=3.0,
        anchorage_exit_dist=4.0,
        stopped_begin_speed=0.2,
        stopped_end_speed=0.5,
        min_gap_minutes=min_gap_minutes,
        start_date=datetime.date(2024, 1, 2),
        end_date=datetime.date(2024, 1, 3),
    )
    return transform.thin((("seg-1", "2024-01-02"), records), anchorage_map())


def minutes_of(records):
    return [int((r.timestamp - at(0)).total_seconds() // 60) for r in records]


def test_keeps_the_positions_around_entering_and_exiting_port_and_the_day_edges():
    records = [
        position(0, **AT_SEA), position(10, **AT_SEA), position(20, **AT_SEA),
        position(30, **IN_PORT), position(40, **IN_PORT), position(50, **IN_PORT),
        position(60, **AT_SEA), position(70, **AT_SEA), position(80, **AT_SEA),
    ]

    assert minutes_of(thin(records)) == [0, 20, 30, 50, 60, 80]


def test_keeps_the_positions_around_stopping_and_starting_in_port():
    records = [
        position(0, **IN_PORT), position(10, **IN_PORT), position(20, **STOPPED),
        position(30, **STOPPED), position(40, **STOPPED), position(50, **IN_PORT),
        position(60, **IN_PORT),
    ]

    assert minutes_of(thin(records)) == [0, 10, 20, 40, 50, 60]


def test_keeps_only_the_day_edges_without_transitions_or_gaps():
    records = [position(minutes, **AT_SEA) for minutes in range(0, 50, 10)]

    assert minutes_of(thin(records)) == [0, 40]


def test_keeps_the_positions_around_a_gap_and_flags_its_end():
    records = [
        position(0, **AT_SEA), position(10, **AT_SEA), position(20, **AT_SEA),
        position(300, **AT_SEA), position(310, **AT_SEA), position(320, **AT_SEA),
    ]

    kept = thin(records, min_gap_minutes=240)

    assert minutes_of(kept) == [0, 20, 300, 320]
    assert [r.is_possible_gap_end for r in kept] == [True, False, True, False]


def test_the_first_position_of_the_day_may_end_a_gap_from_the_previous_day():
    kept = thin([position(0, **AT_SEA), position(10, **AT_SEA)])

    assert [r.is_possible_gap_end for r in kept] == [True, False]


def test_positions_carry_the_nearest_anchorage_within_reach_even_out_of_port():
    (kept,) = thin([position(0, **AT_SEA)])

    assert kept.port_s2id == "port-1"
    assert (kept.port_lat, kept.port_lon) == (PORT_LAT, PORT_LON)
    assert kept.port_dist == pytest.approx(10.0, abs=0.1)


def test_positions_out_of_reach_of_any_anchorage_have_no_port():
    (kept,) = thin([position(0, **FAR_AWAY)])

    assert (kept.port_s2id, kept.port_dist, kept.port_lat, kept.port_lon) == (None,) * 4


def test_between_entry_and_exit_distances_a_vessel_keeps_its_previous_state():
    between = dict(km=3.5, speed=5.0)
    records = [
        position(0, **AT_SEA), position(10, **between), position(20, **AT_SEA),  # never in port
        position(30, **AT_SEA),
    ]

    assert minutes_of(thin(records)) == [0, 30]


def test_min_gap_must_be_under_one_day():
    with pytest.raises(AssertionError, match="min gap must be under one day"):
        thin([], min_gap_minutes=24 * 60)
