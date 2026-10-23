import datetime
from collections import namedtuple
from typing import Optional

from .core.latlon import LatLon
from .core.namedtuples import s_to_datetime


def is_location_message(msg):
    return msg["lat"] is not None and msg["lon"] is not None and msg["speed"] is not None


def has_valid_location(msg):
    return -90 <= msg["lat"] <= 90 and -180 <= msg["lon"] <= 180 and 0 <= msg["speed"] <= 102.2


def has_destination(msg):
    return msg["destination"] not in ("", None)


class VesselRecord:
    """Base class for vessel records created from messages."""

    @staticmethod
    def from_msg(msg):
        """Builds the record a message makes: a location, an info, or an invalid record.

        Each subclass overrides it with its own constructor; this one picks which.
        """
        if is_location_message(msg) and has_valid_location(msg):
            return VesselLocationRecord.from_msg(msg)
        elif has_destination(msg):
            return VesselInfoRecord.from_msg(msg)
        else:
            return InvalidRecord.from_msg(msg)


class InvalidRecord(namedtuple("InvalidRecord", ["identifier", "timestamp"]), VesselRecord):

    __slots__ = ()

    @staticmethod
    def from_msg(msg):
        return InvalidRecord(
            identifier=msg["ident"],
            timestamp=s_to_datetime(msg["timestamp"]),
        )


class VesselInfoRecord(
    namedtuple("VesselInfoRecord", ["identifier", "timestamp", "destination"]),
    VesselRecord,
):

    __slots__ = ()

    @staticmethod
    def from_msg(msg):
        return VesselInfoRecord(
            identifier=msg["ident"],
            timestamp=s_to_datetime(msg["timestamp"]),
            destination=msg["destination"],
        )


class VesselLocationRecord(
    namedtuple(
        "VesselLocationRecord",
        ["identifier", "timestamp", "location", "speed", "destination"],
    ),
    VesselRecord,
):

    __slots__ = ()
    __annotations__ = {
        "identifier": str,
        "timestamp": datetime.datetime,
        "location": LatLon,
        "speed": float,
        "destination": Optional[str],
    }

    @staticmethod
    def from_msg(msg):
        latlon = LatLon(msg["lat"], msg["lon"])
        return VesselLocationRecord(
            identifier=msg["ident"],
            timestamp=s_to_datetime(msg["timestamp"]),
            location=latlon,
            speed=msg["speed"],
            destination=None,
        )
