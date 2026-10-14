import os
from importlib.resources import files

import apache_beam as beam

from pipe_anchorages.assets.data import port_lists
from pipe_anchorages.core.named_anchorage_point import NamedAnchoragePoint
from pipe_anchorages.get_override_list import get_override_list
from pipe_anchorages.port_info_finder import normalize_label


class CreateOverrideAnchorages(beam.PTransform):

    _override_list = None

    def __init__(self, override_path, used_s2ids):
        self.override_path = override_path
        self.used_s2ids = used_s2ids

    def create_override_anchorages(self, dummy, used_s2ids):
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

    def expand(self, p):
        return (
            p
            | beam.Create([None])
            | beam.FlatMap(self.create_override_anchorages, self.used_s2ids)
        )
