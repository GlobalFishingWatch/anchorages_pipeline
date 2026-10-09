import datetime
from typing import NamedTuple, Optional

from pipe_anchorages.common import LatLon


class VisitLocationRecord(NamedTuple):
    identifier: str
    timestamp: datetime.datetime
    location: LatLon
    speed: float
    is_possible_gap_end: bool
    port_s2id: Optional[str]
    port_dist: Optional[float]
    port_lon: Optional[float]
    port_lat: Optional[float]
