import math

import numpy as np

EARTH_RADIUS = 6371  # kilometers

inf = float("inf")
assert math.isinf(inf)


def distance(a, b):
    """The haversine distance in km between the LatLons `a` and `b`.

    distances() is the same for many points at once: change both together.
    """
    h = (
        math.sin(math.radians(a.lat - b.lat) / 2) ** 2
        + math.cos(math.radians(a.lat))
        * math.cos(math.radians(b.lat))
        * math.sin(math.radians((a.lon - b.lon)) / 2) ** 2
    )
    h = min(h, 1)
    return 2 * EARTH_RADIUS * math.asin(math.sqrt(h))


def distances(loc, lat_rad, lon_rad, cos_lat):
    """The haversine distances in km between the LatLon `loc` and many points, as a numpy array.

    The points are given as numpy arrays of their latitudes and longitudes in radians, and of
    their latitudes' cosines, so they can be computed once for many calls.

    The vectorized form of distance(): change both together. Its results can differ from
    distance()'s in the last bits, since numpy's sin and arcsin and the order of the operations
    aren't the same.
    """
    lat = math.radians(loc.lat)
    lon = math.radians(loc.lon)
    h = (
        np.sin((lat - lat_rad) / 2) ** 2
        + math.cos(lat) * cos_lat * np.sin((lon - lon_rad) / 2) ** 2
    )
    return 2 * EARTH_RADIUS * np.arcsin(np.sqrt(np.minimum(h, 1)))
