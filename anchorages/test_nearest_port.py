from collections import namedtuple
from .anchorages import LatLon, S2_cell_ID, distance
from .nearest_port import port_finder, Port


def test_nearest_port():
    assert port_finder(LatLon(37.8, -122.4)) == Port(name='SAN FRANCISCO', country='US', lat=37.816667, lon=-122.416667)
    assert port_finder(LatLon(1.3521, 103.8198)) == Port(name='KEPPEL - (EAST SINGAPORE)', country='SG', lat=1.283333, lon=103.85)
    assert port_finder(LatLon(59.3293, 18.0686)) == Port(name='STOCKHOLM', country='SE', lat=59.333333, lon=18.05)
    assert port_finder(LatLon(-90, 0)) == Port(name='MCMURDO STATION', country='AQ', lat=-77.85, lon=166.65)


FauxLocationRecord = namedtuple('FauxLocationRecord',
            ['s2id', 'location'])


def test_is_within():
    base_lat = 37.816667
    lon = -122.416667
    results = []
    for delta in [0.001, 0.01, 0.1, 1]:
        lat = base_lat + delta
        loc= LatLon(lat, lon)
        fvlr = FauxLocationRecord(S2_cell_ID(loc), loc)
        port = port_finder.is_within(5, fvlr)
        if port:
            dist = distance(loc, LatLon(port.lat, port.lon))
            results.append((dist, port))
        else:
            results.append(None)
    assert results == [
        (0.11119492664429959, Port(name='SAN FRANCISCO', country='US', lat=37.816667, lon=-122.416667)),
        (1.1119492664453663, Port(name='SAN FRANCISCO', country='US', lat=37.816667, lon=-122.416667)),
        (4.386113626710013, Port(name='POINT RICHMOND', country='US', lat=37.916667, lon=-122.366667)),
        None,
    ]


        # def is_within(self, range, location_record):
        # loc = location_record.location
        # if location_record.s2id not in self.ports_near:
        #     ports = []
        #     for p in self.ports:
        #         dist = distance(p, loc)
        #         if dist <= self.buffer_km:
        #             ports.append(p)
        #     self.ports_near[location_record.s2id] = ports
        # candidates = sorted([(distance(p, loc), p) for p in self.ports_near[location_record.s2id]])
        # if candidates:
        #     dist, port = candidates[0]
        #     if dist <= range:
        #         return p
        # return None