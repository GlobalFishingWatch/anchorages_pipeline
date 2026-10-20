import datetime
from typing import NamedTuple, Optional

from pipe_anchorages.common import LatLon
from pipe_anchorages.core.namedtuples import epoch

_MICROSECOND = datetime.timedelta(microseconds=1)


class VisitLocationRecord(NamedTuple):
    """A position kept by port-state-transitions, with its nearest anchorage.

    The port fields are None when no anchorage is within reach. port-state-transitions builds
    them with the seg_id as identifier; port-visits reads them back from its output table with
    (ssvid, vessel_id, seg_id) as identifier.
    """

    identifier: str
    timestamp: datetime.datetime
    location: LatLon
    speed: float
    is_possible_gap_end: bool
    port_s2id: Optional[str]
    port_dist: Optional[float]
    port_lon: Optional[float]
    port_lat: Optional[float]

    def to_shuffle(self) -> tuple:
        """Returns port-visits' record as a plain tuple, without the vessel_id.

        For port-visits' GroupByKey by vessel_id, whose key holds it: Beam encodes a tuple of
        primitives with its fast coder, but pickles a NamedTuple value (~3x larger and ~10x
        slower). The timestamp travels as integer microseconds, so from_shuffle rebuilds the
        record exactly.
        """
        ssvid, _, seg_id = self.identifier
        return (
            ssvid,
            seg_id,
            (self.timestamp - epoch) // _MICROSECOND,
            self.location.lat,
            self.location.lon,
            self.speed,
            self.is_possible_gap_end,
            self.port_s2id,
            self.port_dist,
            self.port_lon,
            self.port_lat,
        )

    @classmethod
    def from_shuffle(cls, vessel_id: str, fields: tuple) -> "VisitLocationRecord":
        """Rebuilds the record of `vessel_id` from the tuple to_shuffle returned."""
        (ssvid, seg_id, micros, lat, lon, speed, is_possible_gap_end,
         port_s2id, port_dist, port_lon, port_lat) = fields
        return cls(
            identifier=(ssvid, vessel_id, seg_id),
            timestamp=epoch + micros * _MICROSECOND,
            location=LatLon(lat, lon),
            speed=speed,
            is_possible_gap_end=is_possible_gap_end,
            port_s2id=port_s2id,
            port_dist=port_dist,
            port_lon=port_lon,
            port_lat=port_lat,
        )
