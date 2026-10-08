import math

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to

from pipe_anchorages.common import LatLon
from pipe_anchorages.core.port_visit import PortVisit
from pipe_anchorages.core.visit_event import VisitEvent
from pipe_anchorages.pipelines.port_visits.transforms.core import (
    DetectPortVisits,
    event_to_msg,
    from_msg,
    visit_to_msg,
)

from .factories import T0, at


def row(**overrides):
    """A row shaped like the query's output."""
    return dict(
        ssvid="111",
        vessel_id="vessel-1",
        seg_id="seg-1",
        lat=-34.59,
        lon=-58.4,
        speed=5.0,
        is_possible_gap_end=False,
        port_s2id="a1",
        port_dist=1.0,
        port_lon=-58.4,
        port_lat=-34.6,
        timestamp=T0.timestamp(),
    ) | overrides


def event(event_type, minutes):
    return VisitEvent(
        anchorage_id="a1", lat=-34.6, lon=-58.4, vessel_lat=-34.59, vessel_lon=-58.4,
        ssvid="111", seg_id="seg-1", vessel_id="vessel-1", timestamp=at(minutes),
        event_type=event_type, last_timestamp=at(minutes - 10),
    )


def test_from_msg_keys_the_record_by_vessel_id():
    vessel_id, rcd = from_msg(row())

    assert vessel_id == "vessel-1"
    assert rcd.identifier == ("111", "vessel-1", "seg-1")
    assert rcd.timestamp == T0
    assert rcd.location == LatLon(-34.59, -58.4)
    assert (rcd.port_s2id, rcd.port_dist, rcd.speed) == ("a1", 1.0, 5.0)


def test_from_msg_puts_records_with_no_anchorage_nearby_infinitely_far():
    _, rcd = from_msg(row(port_dist=None))

    assert rcd.port_dist == math.inf


def test_from_msg_does_not_modify_the_row():
    original = row()

    from_msg(original)

    assert original == row()


def test_event_to_msg_drops_vessel_fields_and_converts_the_timestamp():
    assert event_to_msg(event("PORT_ENTRY", 10)) == dict(
        anchorage_id="a1", lat=-34.6, lon=-58.4, vessel_lat=-34.59, vessel_lon=-58.4,
        seg_id="seg-1", timestamp=at(10).timestamp(), event_type="PORT_ENTRY",
    )


def test_visit_to_msg_converts_timestamps_and_events():
    events = [event("PORT_ENTRY", 10), event("PORT_EXIT", 20)]
    visit = PortVisit(
        visit_id="v", ssvid="111", vessel_id="vessel-1",
        start_timestamp=at(10), start_lat=-34.6, start_lon=-58.4, start_anchorage_id="a1",
        end_timestamp=at(20), end_lat=-34.6, end_lon=-58.4, end_anchorage_id="a1",
        duration_hrs=1 / 6, events=events, confidence=1,
    )

    msg = visit_to_msg(visit)

    assert msg["start_timestamp"] == at(10).timestamp()
    assert msg["end_timestamp"] == at(20).timestamp()
    assert msg["events"] == [event_to_msg(e) for e in events]
    assert msg["confidence"] == 1


def test_detect_port_visits_turns_rows_into_visit_rows(pipeline):
    rows = [
        row(port_dist=10.0, speed=10.0, timestamp=at(0).timestamp()),
        row(port_dist=1.0, speed=5.0, timestamp=at(10).timestamp()),
        row(port_dist=10.0, speed=10.0, timestamp=at(20).timestamp()),
    ]

    with pipeline as p:
        visits = p | beam.Create(rows) | DetectPortVisits(
            anchorage_entry_dist_km=3.0,
            anchorage_exit_dist_km=4.0,
            stopping_speed_knots=0.2,
            starting_speed_knots=0.5,
            min_anchorage_gap_minutes=60,
            max_inter_seg_dist_nm=60.0,
            end_time=at(24 * 60),
        )
        summary = visits | beam.Map(
            lambda v: (v["vessel_id"], v["confidence"], [e["event_type"] for e in v["events"]])
        )
        assert_that(summary, equal_to([("vessel-1", 1, ["PORT_ENTRY", "PORT_EXIT"])]))
