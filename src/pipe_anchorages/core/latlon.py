from typing import NamedTuple

import s2sphere


class LatLon(NamedTuple):

    lat: float
    lon: float

    def S2CellId(self, scale=None):
        ll = s2sphere.LatLng.from_degrees(self.lat, self.lon)
        cellid = s2sphere.CellId.from_lat_lng(ll)
        if scale is not None:
            cellid = cellid.parent(scale)
        return cellid
