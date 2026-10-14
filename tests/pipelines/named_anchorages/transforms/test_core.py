from pipe_anchorages import common as cmn
from pipe_anchorages.core.named_anchorage_location import NamedAnchorageLocation
from pipe_anchorages.pipelines.named_anchorages.transforms.core import (
    is_not_removed,
    named_anchorage_to_msg,
)

from .factories import HIGH_SEAS_LAT, HIGH_SEAS_LON, anchorage_location, named_anchorage


def test_a_query_row_becomes_a_named_anchorage_location_with_no_names():
    anchorage = NamedAnchorageLocation.from_msg(anchorage_location())

    assert anchorage.s2id == "s2id-1"
    assert anchorage.mean_location == cmn.LatLon(HIGH_SEAS_LAT, HIGH_SEAS_LON)
    assert anchorage.rms_drift_radius == 0.1
    assert anchorage.total_visits == 10
    assert anchorage.active_ssvids == 5
    assert anchorage.total_ssvids == 6
    assert (anchorage.label, anchorage.sublabel, anchorage.iso3, anchorage.label_source) == (
        None, None, None, None
    )


def test_the_vessel_counts_are_kept_as_lists_of_that_length():
    anchorage = named_anchorage()

    assert anchorage.vessels == [None] * 3
    assert anchorage.fishing_vessels == [None] * 1


def test_a_named_anchorage_becomes_an_output_row_with_the_input_stats():
    anchorage = named_anchorage()._replace(
        label="PORT", sublabel="PIER", iso3="ARG", label_source="WPI_ports"
    )

    row = named_anchorage_to_msg(anchorage)

    assert row == dict(
        anchorage_location(), label="PORT", sublabel="PIER", iso3="ARG", label_source="WPI_ports"
    )


def test_only_the_anchorages_the_overrides_label_remove_are_removed():
    anchorage = named_anchorage()
    removed = anchorage._replace(label="REMOVE", label_source="anchorage_overrides")

    assert not is_not_removed(removed)
    assert is_not_removed(anchorage._replace(label="REMOVE", label_source="top_destination"))
    assert is_not_removed(anchorage._replace(label="PORT", label_source="anchorage_overrides"))
