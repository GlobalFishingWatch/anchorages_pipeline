import fiona
from shapely.geometry import shape, mapping, Point, Polygon, MultiPolygon


_cache = {}


def get_iso3_finder(shapefile_path):
    if shapefile_path not in _cache:
        _cache[shapefile_path] = Iso3Finder(shapefile_path)
    return _cache[shapefile_path]


class Iso3Finder(object):
    def __init__(self, shapefile_path):
        self.shapes = []
        self.iso3s = []
        for x in fiona.open(shapefile_path):
            shp = shape(x["geometry"])
            self.shapes.append(shp)
            ter = x["properties"]["ISO_TER1"]
            sov = x["properties"]["ISO_SOV1"]
            self.iso3s.append(ter if ter else sov)

    def iso3(self, lat, lon):
        pt = Point(lon, lat)
        for shp, iso3 in zip(self.shapes, self.iso3s):
            if shp.intersects(pt):
                return iso3
        return None
