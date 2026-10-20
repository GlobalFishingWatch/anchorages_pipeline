import numpy as np

from pipe_anchorages.distance import distances


class AnchorageArrays:
    """The anchorages within reach of one S2 cell, with their coordinates as numpy arrays.

    Built once per S2 cell in the side input, so that finding a position's nearest anchorage
    is one vectorized haversine over all of them, instead of a pure-Python loop per anchorage.
    The anchorages are sorted by s2id and argmin returns the first minimum, so ties resolve to
    the lowest s2id, as the loop this replaces did. The distances come from
    pipe_anchorages.distance.distances, and can differ from distance()'s in the last bits.
    """

    __slots__ = ("anchorages", "lat_rad", "lon_rad", "cos_lat")

    def __init__(self, anchorages) -> None:
        self.anchorages = sorted(anchorages, key=lambda x: x.s2id)
        self.lat_rad = np.radians([a.mean_location.lat for a in self.anchorages])
        self.lon_rad = np.radians([a.mean_location.lon for a in self.anchorages])
        self.cos_lat = np.cos(self.lat_rad)

    def nearest(self, loc):
        """Returns the nearest anchorage to `loc` and its distance in km."""
        km = distances(loc, self.lat_rad, self.lon_rad, self.cos_lat)
        i = int(np.argmin(km))
        return self.anchorages[i], float(km[i])


# No type hints: Beam maps it, and would infer coders from them.
def to_anchorage_arrays(item):
    """Converts a (S2 token, anchorages) pair of CreateTaggedAnchorages into AnchorageArrays."""
    token, anchorages = item
    return token, AnchorageArrays(anchorages)
