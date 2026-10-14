from importlib.resources import files

import apache_beam as beam

from pipe_anchorages.assets.data import port_lists
from pipe_anchorages.get_override_list import get_override_list


class FindUsedS2ids(beam.PTransform):

    _override_list = None

    def __init__(self, override_path):
        self.override_path = override_path
        self.s2ids_in_overrides = set(
            row["s2id"]
            for row in get_override_list(files(port_lists).joinpath(self.override_path))
        )

    def find_used_s2ids(self, named_anchorage):
        if named_anchorage.s2id in self.s2ids_in_overrides:
            yield named_anchorage.s2id

    def expand(self, anchorages):
        return anchorages | beam.FlatMap(self.find_used_s2ids)
