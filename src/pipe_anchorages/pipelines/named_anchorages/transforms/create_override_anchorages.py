import os
from importlib.resources import files

import apache_beam as beam

from pipe_anchorages.assets.data import port_lists
from pipe_anchorages.core.named_anchorage_point import NamedAnchoragePoint
from pipe_anchorages.get_override_list import get_override_list
from pipe_anchorages.port_info_finder import normalize_label


class CreateOverrideAnchorages(beam.PTransform):
    """Creates a NamedAnchoragePoint for each anchorage of the overrides list at `override_path`
    whose s2id isn't in `used_s2ids` (a side input, see FindUsedS2ids).

    They take the override's location, names and iso3, have no stats, and their label_source is
    the list's file name ("anchorage_overrides"). Applied to the pipeline, not to a PCollection.
    """

    def __init__(self, override_path: str, used_s2ids: beam.pvalue.AsSideInput) -> None:
        self.override_path = override_path
        self.used_s2ids = used_s2ids

    # No type hints on the methods Beam maps: it would infer coders from them.
    def create_override_anchorages(self, dummy, used_s2ids):
        """Yields the overrides' anchorages whose s2id isn't in `used_s2ids`."""
        used_s2ids = set(used_s2ids)
        for row in get_override_list(files(port_lists).joinpath(self.override_path)):
            if row["s2id"] not in used_s2ids:
                yield NamedAnchoragePoint(
                    mean_location=row["latLon"],
                    total_visits=None,
                    vessels=frozenset([]),
                    fishing_vessels=frozenset([]),
                    rms_drift_radius=None,
                    top_destination=None,
                    s2id=row["s2id"],
                    neighbor_s2ids=None,
                    active_ssvids=None,
                    total_ssvids=None,
                    stationary_ssvid_days=None,
                    stationary_fishing_ssvid_days=None,
                    active_ssvid_days=None,
                    label=normalize_label(row["label"]),
                    sublabel=normalize_label(row["sublabel"]),
                    label_source=os.path.splitext(os.path.basename(self.override_path))[0],
                    iso3=row["iso3"],
                )

    def expand(self, p: beam.Pipeline) -> beam.PCollection:
        return (
            p
            | beam.Create([None])
            | beam.FlatMap(self.create_override_anchorages, self.used_s2ids)
        )
