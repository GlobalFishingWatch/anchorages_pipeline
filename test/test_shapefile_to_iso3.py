import os
from pipe_anchorages.shapefile_to_iso3 import Iso3Finder

def test():

    parent_dir = os.path.dirname(os.path.dirname(__file__))

    items = [
      (37.7749,-140, None),
     (37.7749, -122.4194, 'USA'),
     (31.2304, 121.4737, 'CHN'),
     (51.5074, -0.1278, 'GBR'),
     (-33.8688, 151.2093, 'AUS')
    ]
    finder = Iso3Finder(os.path.join(parent_dir, "pipe_anchorages/data/EEZ/EEZ_land_v2_201410.shp"))
    found = []
    expected = []
    for lat, lon, iso3 in items:
        found.append(finder.iso3(lat, lon))
        expected.append(iso3)

    assert expected == found

test()