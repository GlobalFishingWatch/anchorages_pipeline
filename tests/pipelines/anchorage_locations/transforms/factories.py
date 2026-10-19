"""Builders for the records these transforms work on."""

import datetime

from pipe_anchorages.common import LatLon
from pipe_anchorages.records import VesselInfoRecord, VesselLocationRecord

T0 = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)


def at(minutes):
    """A timezone-aware timestamp `minutes` after T0, as s_to_datetime() produces."""
    return T0 + datetime.timedelta(minutes=minutes)


def location(ident, minutes, lat=-34.6, lon=-58.4, speed=0.0, destination=None):
    return VesselLocationRecord(
        identifier=ident, timestamp=at(minutes), location=LatLon(lat, lon), speed=speed,
        destination=destination,
    )


def info(ident, minutes, destination):
    return VesselInfoRecord(identifier=ident, timestamp=at(minutes), destination=destination)
