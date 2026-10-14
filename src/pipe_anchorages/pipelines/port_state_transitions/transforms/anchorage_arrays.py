import math

import numpy as np

from pipe_anchorages.distance import EARTH_RADIUS


class AnchorageArrays:
    """The anchorages within reach of one S2 cell, with their coordinates as numpy arrays.

    Built once per S2 cell in the side input, so that finding a position's nearest anchorage
    is one vectorized haversine over all of them, instead of a pure-Python loop per anchorage.
    The anchorages are sorted by s2id and argmin returns the first minimum, so ties resolve as
    the loop did. Distances can differ from pipe_anchorages.distance.distance in the last bits.
    """

    __slots__ = ("anchorages", "lat_rad", "lon_rad", "cos_lat")

    def __init__(self, anchorages) -> None:
        self.anchorages = sorted(anchorages, key=lambda x: x.s2id)
        self.lat_rad = np.radians([a.mean_location.lat for a in self.anchorages])
        self.lon_rad = np.radians([a.mean_location.lon for a in self.anchorages])
        self.cos_lat = np.cos(self.lat_rad)

    def nearest(self, loc):
        """Returns the nearest anchorage to `loc` and its distance in km."""
        lat = math.radians(loc.lat)
        lon = math.radians(loc.lon)
        h = (
            np.sin((lat - self.lat_rad) / 2) ** 2
            + math.cos(lat) * self.cos_lat * np.sin((lon - self.lon_rad) / 2) ** 2
        )
        distances = 2 * EARTH_RADIUS * np.arcsin(np.sqrt(np.minimum(h, 1)))
        i = int(np.argmin(distances))
        return self.anchorages[i], float(distances[i])


# No type hints: Beam maps it, and would infer coders from them.
def to_anchorage_arrays(item):
    """Converts a (S2 token, anchorages) pair of CreateTaggedAnchorages into AnchorageArrays."""
    token, anchorages = item
    return token, AnchorageArrays(anchorages)
