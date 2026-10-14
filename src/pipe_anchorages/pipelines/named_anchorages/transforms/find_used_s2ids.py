from importlib.resources import files

import apache_beam as beam

from pipe_anchorages.assets.data import port_lists
from pipe_anchorages.get_override_list import get_override_list


class FindUsedS2ids(beam.PTransform):
    """Returns the s2ids of the anchorages that are in the overrides list at `override_path`.

    CreateOverrideAnchorages adds the overrides' other anchorages.
    """

    def __init__(self, override_path: str) -> None:
        self.override_path = override_path
        self.s2ids_in_overrides = set(
            row["s2id"]
            for row in get_override_list(files(port_lists).joinpath(self.override_path))
        )

    # No type hints on the methods Beam maps: it would infer coders from them.
    def find_used_s2ids(self, named_anchorage):
        """Yields the s2id of `named_anchorage` if it's in the overrides list."""
        if named_anchorage.s2id in self.s2ids_in_overrides:
            yield named_anchorage.s2id

    def expand(self, anchorages: beam.PCollection) -> beam.PCollection:
        return anchorages | beam.FlatMap(self.find_used_s2ids)
