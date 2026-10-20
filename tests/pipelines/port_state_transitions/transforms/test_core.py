from pipe_anchorages.common import LatLon
from pipe_anchorages.core.visit_location_record import VisitLocationRecord
from pipe_anchorages.pipelines.port_state_transitions.transforms.core import (
    record_to_msg,
)

from .factories import PORT_LAT, PORT_LON, T0, at


def test_record_to_msg_flattens_the_location_and_converts_the_timestamp_to_seconds():
    record = VisitLocationRecord(
        identifier="seg-1",
        timestamp=at(10),
        location=LatLon(-34.5, -58.3),
        speed=5.0,
        is_possible_gap_end=True,
        port_s2id="port-1",
        port_dist=1.0,
        port_lon=PORT_LON,
        port_lat=PORT_LAT,
    )

    assert record_to_msg(record) == dict(
        identifier="seg-1",
        timestamp=T0.timestamp() + 600,
        lat=-34.5,
        lon=-58.3,
        speed=5.0,
        is_possible_gap_end=True,
        port_s2id="port-1",
        port_dist=1.0,
        port_lon=PORT_LON,
        port_lat=PORT_LAT,
    )
