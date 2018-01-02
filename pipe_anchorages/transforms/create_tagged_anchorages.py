from __future__ import absolute_import, print_function, division

import apache_beam as beam

from pipe_anchorages import common as cmn
from pipe_anchorages.objects.pseudo_anchorage import PseudoAnchorage


class CombineAnchoragesIntoMap(beam.CombineFn):

    def create_accumulator(self):
        return {}

    def add_input(self, accumulator, item):
        keys, value = item
        for key in keys:
            if key not in accumulator:
                accumulator[key] = set()
            accumulator[key].add(value)
        return accumulator

    def merge_accumulators(self, accumulators):
        result = {}
        for accum in accumulators:
            for (key, val) in accum.iteritems():
                if key not in result:
                    result[key] = set()
                result[key] |= val
        return result

    def extract_output(self, accumulator):
        return accumulator

class CreateTaggedAnchorages(beam.PTransform):

    def dict_to_psuedo_anchorage(self, obj):
        return PseudoAnchorage(
                mean_location = cmn.LatLon(obj['anchor_lat'], obj['anchor_lon']), 
                s2id = obj['anchor_id'], 
                port_name = obj['FINAL_NAME'])

    def tag_anchorage_with_s2ids(self, anchorage):
        central_cell_id = anchorage.mean_location.S2CellId(cmn.VISITS_S2_SCALE)
        s2ids = {central_cell_id.to_token()}
        for cell_id in central_cell_id.get_all_neighbors(cmn.VISITS_S2_SCALE):
            s2ids.add(cell_id.to_token())
        return (s2ids, anchorage)

    def expand(self, anchorages_text):
        return (anchorages_text
            | beam.Map(self.dict_to_psuedo_anchorage)
            | beam.Map(self.tag_anchorage_with_s2ids)
            | beam.CombineGlobally(CombineAnchoragesIntoMap())
            )

