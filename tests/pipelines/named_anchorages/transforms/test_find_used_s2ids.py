from pipe_anchorages.pipelines.named_anchorages.transforms.find_used_s2ids import FindUsedS2ids

from .factories import POINTE_NOIRE_S2ID, named_anchorage

OVERRIDES = "anchorage_overrides.csv"


def test_the_s2id_of_an_anchorage_in_the_overrides_is_used():
    transform = FindUsedS2ids(OVERRIDES)

    used = transform.find_used_s2ids(named_anchorage(s2id=POINTE_NOIRE_S2ID))

    assert list(used) == [POINTE_NOIRE_S2ID]
    assert list(transform.find_used_s2ids(named_anchorage(s2id="not-an-override"))) == []
