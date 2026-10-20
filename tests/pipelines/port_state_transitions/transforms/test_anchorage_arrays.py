import random

import pytest

from pipe_anchorages.common import LatLon
from pipe_anchorages.core.pseudo_anchorage import PseudoAnchorage
from pipe_anchorages.distance import distance
from pipe_anchorages.pipelines.port_state_transitions.transforms.anchorage_arrays import (
    AnchorageArrays,
    to_anchorage_arrays,
)

from .factories import KM_PER_DEGREE_LAT, PORT_LAT, PORT_LON


def anchorage(s2id, lat, lon):
    return PseudoAnchorage(mean_location=LatLon(lat, lon), s2id=s2id, port_name="BA")


def nearest_by_loop(loc, anchorages):
    """The nearest anchorage and its distance by the scalar haversine, sorted by s2id."""
    # min returns the first minimum, so ties resolve to the lowest s2id.
    closest = min(
        sorted(anchorages, key=lambda x: x.s2id), key=lambda a: distance(loc, a.mean_location)
    )
    return closest, distance(loc, closest.mean_location)


def test_ties_resolve_to_the_lowest_s2id():
    # On the equator, 0.01° east and west of the position: exactly the same distance.
    east = anchorage("b", 0.0, 0.01)
    west = anchorage("a", 0.0, -0.01)

    nearest, km = AnchorageArrays([east, west]).nearest(LatLon(0.0, 0.0))

    assert nearest == west
    assert km == distance(LatLon(0.0, 0.0), west.mean_location)


def test_nearest_agrees_with_the_scalar_haversine():
    rng = random.Random(0)
    degrees = 5 / KM_PER_DEGREE_LAT
    anchorages = [
        anchorage(
            f"{i:08x}",
            PORT_LAT + rng.uniform(-degrees, degrees),
            PORT_LON + rng.uniform(-degrees, degrees),
        )
        for i in range(200)
    ]
    arrays = AnchorageArrays(anchorages)

    for _ in range(500):
        loc = LatLon(
            PORT_LAT + rng.uniform(-2 * degrees, 2 * degrees),
            PORT_LON + rng.uniform(-2 * degrees, 2 * degrees),
        )
        nearest, km = arrays.nearest(loc)
        expected, expected_km = nearest_by_loop(loc, anchorages)

        assert nearest == expected
        assert km == pytest.approx(expected_km, abs=1e-9)


def test_to_anchorage_arrays_keeps_the_token_and_wraps_the_anchorages():
    anchorages = [anchorage("b", PORT_LAT, PORT_LON), anchorage("a", PORT_LAT + 0.01, PORT_LON)]

    token, arrays = to_anchorage_arrays(("token-1", anchorages))

    assert token == "token-1"
    assert isinstance(arrays, AnchorageArrays)
    assert arrays.anchorages == sorted(anchorages, key=lambda x: x.s2id)
