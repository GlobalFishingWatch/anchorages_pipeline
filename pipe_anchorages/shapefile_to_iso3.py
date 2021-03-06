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
            shp = shape(x['geometry'])
            self.shapes.append(shp)
            self.iso3s.append(x['properties']['ISO_3digit'])

    def iso3(self, lat, lon):
        pt = Point(lon, lat)
        for shp, iso3 in zip(self.shapes, self.iso3s):
            if shp.contains(pt): #pt.within(shp):
                return iso3
        return None
