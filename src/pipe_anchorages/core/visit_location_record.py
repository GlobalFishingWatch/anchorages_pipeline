import datetime
from typing import NamedTuple, Optional

from pipe_anchorages.common import LatLon


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
