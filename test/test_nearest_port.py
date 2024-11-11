import os
from pipe_anchorages.common import LatLon
from pipe_anchorages.nearest_port import PortFinder, Port

this_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(this_dir)

wpi_finder = PortFinder(os.path.join(parent_dir, "pipe_anchorages/data/port_lists/WPI_ports.csv"))


def test_nearest_port():
    assert wpi_finder(LatLon(37.8, -122.4)) == Port(
        label="SAN FRANCISCO",
        sublabel="",
        iso3="USA",
        lat=37.816666999999995,
        lon=-122.41666699999999,
    )
    assert wpi_finder(LatLon(1.3521, 103.8198)) == Port(
        label="KEPPEL - (EAST SINGAPORE)", sublabel="", iso3="SGP", lat=1.283333, lon=103.85
    )
    assert wpi_finder(LatLon(59.3293, 18.0686)) == Port(
        label="STOCKHOLM", iso3="SWE", sublabel="", lat=59.333332999999996, lon=18.05
    )
    assert wpi_finder(LatLon(-90, 0)) == Port(
        label="MCMURDO STATION", iso3="ATA", sublabel="", lat=-77.85, lon=166.65
    )
