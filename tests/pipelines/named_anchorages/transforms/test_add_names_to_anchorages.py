import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to

from pipe_anchorages.pipelines.named_anchorages.transforms.add_names_to_anchorages import (
    AddNamesToAnchorages,
)

from .factories import (
    EAST_CHINA_SEA_LAT,
    EAST_CHINA_SEA_LON,
    KM_PER_DEGREE_LAT,
    POINTE_NOIRE_LAT,
    POINTE_NOIRE_LON,
    PORT_INFO_CONFIG,
    named_anchorage,
)

SHAPEFILE = "EEZ_Land_v3_202030.shp"


def add_names(anchorage):
    transform = AddNamesToAnchorages(SHAPEFILE, PORT_INFO_CONFIG)
    return transform.add_iso3(transform.add_best_label(anchorage))


def names(anchorage):
    return (anchorage.label, anchorage.sublabel, anchorage.iso3, anchorage.label_source)


def test_an_anchorage_at_a_place_gets_its_label_and_sublabel():
    anchorage = named_anchorage(lat=POINTE_NOIRE_LAT, lon=POINTE_NOIRE_LON)

    assert names(add_names(anchorage)) == (
        "POINTE NOIRE", "POINTE NOIRE OFFSHORE", "COG", "anchorage_overrides"
    )


def test_an_anchorage_beyond_the_sublabel_distance_of_a_place_gets_only_its_label():
    anchorage = named_anchorage(lat=POINTE_NOIRE_LAT + 2 / KM_PER_DEGREE_LAT, lon=POINTE_NOIRE_LON)

    assert names(add_names(anchorage)) == ("POINTE NOIRE", None, "COG", "anchorage_overrides")


def test_an_anchorage_near_a_reference_port_gets_the_port_name():
    anchorage = named_anchorage(lat=48.4, lon=-89.25)

    assert names(add_names(anchorage)) == ("THUNDER BAY", None, "CAN", "WPI_ports")


def test_an_anchorage_far_from_any_place_is_named_after_its_normalized_top_destination():
    anchorage = named_anchorage(top_destination="Tromsø ")

    assert names(add_names(anchorage)) == ("TROMSO", None, None, "top_destination")


def test_an_anchorage_far_from_any_place_without_a_top_destination_is_named_after_its_s2id():
    anchorage = named_anchorage(s2id="abc123")

    assert names(add_names(anchorage)) == ("abc123", None, None, "top_destination")


def test_an_anchorage_with_no_place_gets_the_iso3_of_its_eez():
    anchorage = named_anchorage(lat=-45.0, lon=-62.0, top_destination="SOMEWHERE")

    assert add_names(anchorage).iso3 == "ARG"


def test_an_anchorage_in_china_is_named_after_its_s2id():
    anchorage = named_anchorage(
        s2id="abc123", lat=EAST_CHINA_SEA_LAT, lon=EAST_CHINA_SEA_LON, top_destination="NINGBO"
    )

    assert names(add_names(anchorage)) == ("abc123", None, "CHN", "china_s2id_override")


def test_an_anchorage_in_china_keeps_the_name_the_overrides_give_it():
    anchorage = named_anchorage(lat=31.23, lon=121.49)

    assert names(add_names(anchorage)) == ("SHANGHAI", None, "CHN", "anchorage_overrides")


def test_the_transform_names_each_anchorage(pipeline):
    anchorages = [
        named_anchorage(s2id="pointe-noire", lat=POINTE_NOIRE_LAT, lon=POINTE_NOIRE_LON),
        named_anchorage(s2id="high-seas", top_destination="SOMEWHERE"),
    ]

    with pipeline as p:
        named = (
            p
            | beam.Create(anchorages)
            | AddNamesToAnchorages(SHAPEFILE, PORT_INFO_CONFIG)
            | beam.Map(lambda a: (a.s2id, a.label))
        )
        expected = [("pointe-noire", "POINTE NOIRE"), ("high-seas", "SOMEWHERE")]
        assert_that(named, equal_to(expected))
