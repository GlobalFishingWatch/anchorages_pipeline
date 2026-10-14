import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to

from pipe_anchorages.pipelines.named_anchorages.transforms.find_used_s2ids import FindUsedS2ids

from .factories import POINTE_NOIRE_S2ID, named_anchorage

OVERRIDES = "anchorage_overrides.csv"


def test_the_s2id_of_an_anchorage_in_the_overrides_is_used():
    transform = FindUsedS2ids(OVERRIDES)

    used = transform.find_used_s2ids(named_anchorage(s2id=POINTE_NOIRE_S2ID))

    assert list(used) == [POINTE_NOIRE_S2ID]
    assert list(transform.find_used_s2ids(named_anchorage(s2id="not-an-override"))) == []


def test_the_transform_keeps_only_the_s2ids_of_the_overrides(pipeline):
    anchorages = [named_anchorage(s2id=POINTE_NOIRE_S2ID), named_anchorage(s2id="not-an-override")]

    with pipeline as p:
        used = p | beam.Create(anchorages) | FindUsedS2ids(OVERRIDES)
        assert_that(used, equal_to([POINTE_NOIRE_S2ID]))
