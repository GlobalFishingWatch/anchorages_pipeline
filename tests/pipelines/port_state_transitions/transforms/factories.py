"""Builders for the records these transforms work on."""

import datetime

from pipe_anchorages.common import LatLon
from pipe_anchorages.records import VesselInfoRecord, VesselLocationRecord

T0 = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)

PORT_LAT, PORT_LON = -34.6, -58.4
KM_PER_DEGREE_LAT = 111.2

NAMED_ANCHORAGE = dict(anchor_lat=PORT_LAT, anchor_lon=PORT_LON, anchor_id="port-1", label="BA")


def at(minutes):
    """A timezone-aware timestamp `minutes` after T0."""
    return T0 + datetime.timedelta(minutes=minutes)


def position(minutes, km, speed, seg_id="seg-1"):
    """A position of segment `seg_id`, `km` north of the port."""
    return VesselLocationRecord(
        identifier=seg_id,
        timestamp=at(minutes),
        location=LatLon(PORT_LAT + km / KM_PER_DEGREE_LAT, PORT_LON),
        speed=speed,
        destination=None,
    )


def info(minutes, destination, seg_id="seg-1"):
    """A message of segment `seg_id` with a destination and no position."""
    return VesselInfoRecord(identifier=seg_id, timestamp=at(minutes), destination=destination)
