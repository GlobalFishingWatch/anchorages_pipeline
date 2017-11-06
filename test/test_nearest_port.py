from collections import namedtuple
from anchorages.common import LatLon
from anchorages.distance import distance
from anchorages.nearest_port import wpi_finder, Port


def test_nearest_port():
    assert wpi_finder(LatLon(37.8, -122.4)) == Port(name='SAN FRANCISCO', country='US', lat=37.816667, lon=-122.416667)
    assert wpi_finder(LatLon(1.3521, 103.8198)) == Port(name='KEPPEL - (EAST SINGAPORE)', country='SG', lat=1.283333, lon=103.85)
    assert wpi_finder(LatLon(59.3293, 18.0686)) == Port(name='STOCKHOLM', country='SE', lat=59.333333, lon=18.05)
    assert wpi_finder(LatLon(-90, 0)) == Port(name='MCMURDO STATION', country='AQ', lat=-77.85, lon=166.65)

