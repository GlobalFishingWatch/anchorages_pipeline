from __future__ import absolute_import, print_function, division

import apache_beam as beam
import six

from pipe_anchorages import common as cmn
from pipe_anchorages.objects.pseudo_anchorage import PseudoAnchorage


class CreateTaggedAnchorages(beam.PTransform):

    def dict_to_psuedo_anchorage(self, obj):
        return PseudoAnchorage(
                mean_location = cmn.LatLon(obj['anchor_lat'], obj['anchor_lon']), 
                s2id = obj['anchor_id'], 
                port_name = obj['label'])

    def tag_anchorage_with_s2ids(self, anchorage):
        central_cell_id = anchorage.mean_location.S2CellId(cmn.VISITS_S2_SCALE)
        ids = {anchorage.s2id}
        ids.add(central_cell_id.to_token())
        for cell_id in central_cell_id.get_all_neighbors(cmn.VISITS_S2_SCALE):
            ids.add(cell_id.to_token())
        for s2id in ids:
            yield (s2id, anchorage)


    def expand(self, anchorages_text):
        return (anchorages_text
            | beam.Map(self.dict_to_psuedo_anchorage)
            | beam.FlatMap(self.tag_anchorage_with_s2ids)
            | beam.GroupByKey()
            )

