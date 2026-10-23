import datetime
import math
from typing import NamedTuple, Optional

from pipe_anchorages.common import LatLon


class VisitLocationRecord(NamedTuple):
    """A position kept by port-state-transitions, with its nearest anchorage.

    Port-state-transitions creates these from a segment, so it knows only the seg_id;
    port-visits reads them back joined with segment_info, which is what gives each record its
    ssvid and vessel_id.
    """

    ssvid: Optional[str]
    vessel_id: Optional[str]
    seg_id: str
    timestamp: datetime.datetime
    location: LatLon
    speed: float
    is_possible_gap_end: bool
    port_s2id: Optional[str]
    port_dist: Optional[float]
    port_lon: Optional[float]
    port_lat: Optional[float]

    # No type hints: Beam maps it, and would infer coders from them.
    @classmethod
    def from_msg(cls, msg):
        """Builds a VisitLocationRecord from a row of port-visits' query.

        A missing port_dist (no anchorage nearby) becomes infinity.
        """
        # A copy: Beam's elements mustn't be modified, other transforms may read them too.
        msg = msg.copy()
        msg["timestamp"] = datetime.datetime.fromtimestamp(msg["timestamp"], datetime.UTC)
        msg["location"] = LatLon(msg.pop("lat"), msg.pop("lon"))
        if msg["port_dist"] is None:
            msg["port_dist"] = math.inf

        return cls(**msg)
