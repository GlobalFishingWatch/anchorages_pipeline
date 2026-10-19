import s2sphere

from pipe_anchorages import common
from pipe_anchorages.assets import schemas
from pipe_anchorages.core.anchorage_location import AnchorageLocation
from pipe_anchorages.pipelines.anchorage_locations.transforms.core import encode_anchorage

SCHEMA = schemas.get_schema("anchorage_locations.json")


def anchorage_location_from_token(token, ssvids):
    s2latlon = s2sphere.CellId.from_token(token).to_lat_lng()
    return AnchorageLocation(
        mean_location=common.LatLon(s2latlon.lat().degrees, s2latlon.lng().degrees),
        total_visits=10,
        vessels=tuple(ssvids),
        fishing_vessels=tuple(ssvids[:2]),
        rms_drift_radius=0.2,
        top_destination="",
        s2id=token,
        neighbor_s2ids=tuple(
            s2sphere.CellId.from_token(token).get_all_neighbors(common.ANCHORAGES_S2_SCALE)
        ),
        active_ssvids=2,
        total_ssvids=0,
        stationary_ssvid_days=4.1,
        stationary_fishing_ssvid_days=3.3,
        active_ssvid_days=7.2,
    )


def test_encode_anchorage_matches_the_output_schema():
    encoded = encode_anchorage(anchorage_location_from_token("0d1b968b", [37, 49, 2]))

    assert len(encoded) == len(SCHEMA)
    type_map = {int: "INTEGER", str: "STRING", float: "FLOAT"}
    for k, v in encoded.items():
        field = next(f for f in SCHEMA if f["name"] == k)
        assert type_map[type(v)] == field["type"], (k, type(v), field["type"])
