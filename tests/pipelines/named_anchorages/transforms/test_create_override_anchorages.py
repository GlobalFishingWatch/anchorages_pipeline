from pipe_anchorages import common as cmn
from pipe_anchorages.pipelines.named_anchorages.transforms.create_override_anchorages import (
    CreateOverrideAnchorages,
)

from .factories import (
    N_OVERRIDES,
    POINTE_NOIRE_LAT,
    POINTE_NOIRE_LON,
    POINTE_NOIRE_S2ID,
    REMOVED_S2ID,
)

OVERRIDES = "anchorage_overrides.csv"


def create(used_s2ids):
    transform = CreateOverrideAnchorages(OVERRIDES, used_s2ids=None)
    return list(transform.create_override_anchorages(None, used_s2ids))


def test_an_override_becomes_an_anchorage_with_its_names_and_no_stats():
    anchorages = create(used_s2ids=[])

    (pointe_noire,) = [a for a in anchorages if a.s2id == POINTE_NOIRE_S2ID]

    assert pointe_noire.mean_location == cmn.LatLon(POINTE_NOIRE_LAT, POINTE_NOIRE_LON)
    assert pointe_noire.label == "POINTE NOIRE"
    assert pointe_noire.sublabel == "POINTE NOIRE OFFSHORE"
    assert pointe_noire.iso3 == "COG"
    assert pointe_noire.label_source == "anchorage_overrides"
    assert pointe_noire.total_visits is None
    assert pointe_noire.vessels == frozenset()
    assert pointe_noire.fishing_vessels == frozenset()


def test_every_override_is_created_including_the_one_to_remove():
    anchorages = create(used_s2ids=[])

    assert len(anchorages) == N_OVERRIDES
    assert REMOVED_S2ID in {a.s2id for a in anchorages}


def test_the_overrides_of_the_used_s2ids_are_not_created():
    anchorages = create(used_s2ids=[POINTE_NOIRE_S2ID])

    assert len(anchorages) == N_OVERRIDES - 1
    assert POINTE_NOIRE_S2ID not in {a.s2id for a in anchorages}
