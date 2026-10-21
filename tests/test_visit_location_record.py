import datetime

from pipe_anchorages.common import LatLon
from pipe_anchorages.core.visit_location_record import VisitLocationRecord


def visit_location_record(identifier):
    return VisitLocationRecord(
        identifier=identifier,
        timestamp=datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc),
        location=LatLon(-34.6, -58.4),
        speed=5.0,
        is_possible_gap_end=False,
        port_s2id=None,
        port_dist=None,
        port_lon=None,
        port_lat=None,
    )


def test_the_seg_id_of_a_port_visits_record_is_the_last_part_of_its_identifier():
    record = visit_location_record(("111", "vessel-1", "seg-1"))

    assert record.seg_id == "seg-1"


def test_the_seg_id_of_a_port_state_transitions_record_is_its_identifier():
    record = visit_location_record("seg-1")

    assert record.seg_id == "seg-1"
