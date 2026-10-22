"""The visit location record's shape: the segment is always known, the vessel not always."""

import datetime

from pipe_anchorages.common import LatLon
from pipe_anchorages.core.visit_location_record import VisitLocationRecord


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
