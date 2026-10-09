from __future__ import absolute_import, print_function, division

import apache_beam as beam

from pipe_anchorages import common as cmn
from pipe_anchorages.core.pseudo_anchorage import PseudoAnchorage


class CreateTaggedAnchorages(beam.PTransform):
    """Indexes the named anchorages by the S2 cells they can be reached from.

    Input: named anchorages query rows (anchor_lat, anchor_lon, anchor_id, label).
    Output: (s2 token, anchorages) pairs, for the anchorage's own s2id and for its S2 cell and
    the neighbouring ones at cmn.VISITS_S2_SCALE. SmartThinRecords looks positions up by
    their S2 cell to find the anchorages within reach.
    """

    # No type hints on the methods Beam maps: it would infer coders from them.
    def dict_to_psuedo_anchorage(self, obj):
        return PseudoAnchorage(
            mean_location=cmn.LatLon(obj["anchor_lat"], obj["anchor_lon"]),
            s2id=obj["anchor_id"],
            port_name=obj["label"],
        )

    def tag_anchorage_with_s2ids(self, anchorage):
        central_cell_id = anchorage.mean_location.S2CellId(cmn.VISITS_S2_SCALE)
        ids = {anchorage.s2id}
        ids.add(central_cell_id.to_token())
        for cell_id in central_cell_id.get_all_neighbors(cmn.VISITS_S2_SCALE):
            ids.add(cell_id.to_token())
        for s2id in ids:
            yield (s2id, anchorage)

    def expand(self, anchorages_text: beam.PCollection) -> beam.PCollection:
        return (
            anchorages_text
            | beam.Map(self.dict_to_psuedo_anchorage)
            | beam.FlatMap(self.tag_anchorage_with_s2ids)
            | beam.GroupByKey()
        )
