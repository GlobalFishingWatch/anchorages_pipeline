"""The visit location record's shape: the segment is always known, the vessel not always;
and how port-visits builds it from its query's rows."""

import datetime
import math

from pipe_anchorages.common import LatLon
from pipe_anchorages.core.visit_location_record import VisitLocationRecord

T0 = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)


def visit_location_record(ssvid, vessel_id, seg_id="seg-1"):
    """A position, with the vessel's identifiers when they are known."""
    return VisitLocationRecord(
        ssvid=ssvid,
        vessel_id=vessel_id,
        seg_id=seg_id,
        timestamp=datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc),
        location=LatLon(-34.6, -58.4),
        speed=5.0,
        is_possible_gap_end=False,
        port_s2id=None,
        port_dist=None,
        port_lon=None,
        port_lat=None,
    )


def test_a_record_carries_its_vessel_and_its_segment():
    record = visit_location_record("111", "vessel-1", seg_id="seg-2")

    assert (record.ssvid, record.vessel_id, record.seg_id) == ("111", "vessel-1", "seg-2")


def test_a_record_of_a_segment_carries_no_vessel():
    record = visit_location_record(None, None)

    assert (record.ssvid, record.vessel_id, record.seg_id) == (None, None, "seg-1")


def row(**overrides):
    """A row shaped like port-visits' query output."""
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


def test_from_msg_builds_the_record_from_the_row():
    rcd = VisitLocationRecord.from_msg(row())

    assert (rcd.ssvid, rcd.vessel_id, rcd.seg_id) == ("111", "vessel-1", "seg-1")
    assert rcd.timestamp == T0
    assert rcd.location == LatLon(-34.59, -58.4)
    assert (rcd.port_s2id, rcd.port_dist, rcd.speed) == ("a1", 1.0, 5.0)


def test_from_msg_puts_records_with_no_anchorage_nearby_infinitely_far():
    rcd = VisitLocationRecord.from_msg(row(port_dist=None))

    assert rcd.port_dist == math.inf


def test_from_msg_does_not_modify_the_row():
    original = row()

    VisitLocationRecord.from_msg(original)

    assert original == row()
