from pipe_anchorages import common as cmn
from pipe_anchorages.pipelines.port_state_transitions.transforms.create_tagged_anchorages import (
    CreateTaggedAnchorages,
)

from .factories import NAMED_ANCHORAGE, PORT_LAT, PORT_LON


def test_a_query_row_becomes_a_pseudo_anchorage():
    anchorage = CreateTaggedAnchorages().dict_to_psuedo_anchorage(NAMED_ANCHORAGE)

    assert anchorage.mean_location == cmn.LatLon(PORT_LAT, PORT_LON)
    assert anchorage.s2id == "port-1"
    assert anchorage.port_name == "BA"


def test_an_anchorage_is_tagged_with_its_s2id_its_cell_and_the_neighbouring_cells():
    transform = CreateTaggedAnchorages()
    anchorage = transform.dict_to_psuedo_anchorage(NAMED_ANCHORAGE)

    tokens = [s2id for s2id, _ in transform.tag_anchorage_with_s2ids(anchorage)]

    cell = cmn.LatLon(PORT_LAT, PORT_LON).S2CellId(cmn.VISITS_S2_SCALE)
    neighbours = {c.to_token() for c in cell.get_all_neighbors(cmn.VISITS_S2_SCALE)}
    assert sorted(tokens) == sorted({"port-1", cell.to_token(), *neighbours})
