"""Builders for the records these transforms work on."""

import datetime

from pipe_anchorages.common import LatLon
from pipe_anchorages.core.visit_location_record import VisitLocationRecord

T0 = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)

PORT_LAT, PORT_LON = -34.6, -58.4


def at(minutes):
    """A timezone-aware timestamp `minutes` after T0."""
    return T0 + datetime.timedelta(minutes=minutes)


def record(minutes, port_dist, speed, is_possible_gap_end=False, seg_id="seg-1", port="a1"):
    """A port state transition of vessel ("111", "vessel-1"), `port_dist` km from `port`."""
    return VisitLocationRecord(
        identifier=("111", "vessel-1", seg_id),
        timestamp=at(minutes),
        location=LatLon(PORT_LAT + port_dist / 100, PORT_LON),
        speed=speed,
        is_possible_gap_end=is_possible_gap_end,
        port_s2id=port,
        port_dist=port_dist,
        port_lon=PORT_LON,
        port_lat=PORT_LAT,
    )
