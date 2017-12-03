from pipe_anchorages import common
from pipe_anchorages import sparse_inland_mask 

locations = {
    'New York': common.LatLon(40.7128, -74.0059),
    "Chicago": common.LatLon(41.8781, -87.6298),
    "Los Angeles": common.LatLon(34.0522, -118.2437),
    "Phoenix": common.LatLon(33.4484, -112.0740),
    "Scottsdale": common.LatLon(33.4942, -111.9261),
    "Tokyo": common.LatLon(35.6895, 139.6917),
    "Ocean-1": common.LatLon(4, -153),
    "Ocean-2": common.LatLon(0, 179),
    "Ocean-3": common.LatLon(-29, 77),
}

inland_locations = {
    "Chicago": 2336.340987950822,
    "Los Angeles": 574.265826359301,
    "Phoenix": 0,
    "Scottsdale": 14.633197815695059,
}

inland_mask = sparse_inland_mask.SparseInlandMask()

class TestMask(object):

    def test_locations(self):
        for key in sorted(locations):
            is_inland  = key in inland_locations
            assert inland_mask.is_inland(locations[key]) == is_inland, (key, locations[key], is_inland)





