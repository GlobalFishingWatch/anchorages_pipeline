import logging

import apache_beam as beam

from pipe_anchorages import common as cmn
from pipe_anchorages.records import VesselLocationRecord

from .create_tagged_records import CreateTaggedRecords
from .create_anchorage_locations import CreateAnchorageLocations

logger = logging.getLogger(__name__)


def has_location_record(item):
    _, rcd = item
    return isinstance(rcd, VesselLocationRecord)


def encode_anchorage(anchorage) -> dict:
    return {
        "lat": anchorage.mean_location.lat,
        "lon": anchorage.mean_location.lon,
        "total_visits": anchorage.total_visits,
        "drift_radius": anchorage.rms_drift_radius,
        "top_destination": anchorage.top_destination,
        "unique_stationary_ssvid": len(anchorage.vessels),
        "unique_stationary_fishing_ssvid": len(anchorage.fishing_vessels),
        "unique_active_ssvid": anchorage.active_ssvids,
        "unique_total_ssvid": anchorage.total_ssvids,
        "active_ssvid_days": anchorage.active_ssvid_days,
        "stationary_ssvid_days": anchorage.stationary_ssvid_days,
        "stationary_fishing_ssvid_days": anchorage.stationary_fishing_ssvid_days,
        "s2id": anchorage.s2id,
    }


class FindAnchorageLocations(beam.PTransform):
    """Turns position messages into anchorage table records (row dicts).

    Groups the inline chain (CreateVesselRecords -> filter location records ->
    CreateTaggedRecords -> CreateAnchorageLocations -> encode) into one
    composite PTransform, since LinearDag's core slot takes exactly one
    transform. The fishing vessel list is NOT read here: it arrives as a side
    input that LinearDag applies from `side_inputs=` and delivers via
    `set_side_inputs`, which this class implements.

    Constructor params are the raw config fields, not a config object.
    """

    def __init__(self, min_positions, min_duration, max_distance_km, min_unique_vessels) -> None:
        self.min_positions = min_positions
        self.min_duration = min_duration
        self.max_distance_km = max_distance_km
        self.min_unique_vessels = min_unique_vessels
        self._fishing_vessels = None

    def set_side_inputs(self, side_inputs):
        self._fishing_vessels = side_inputs

    def expand(self, xs):
        fishing_vessel_list = beam.pvalue.AsList(self._fishing_vessels)

        return (
            xs
            | cmn.CreateVesselRecords()
            | "FilterOutInfo" >> beam.Filter(has_location_record)
            | CreateTaggedRecords(self.min_positions)
            | CreateAnchorageLocations(
                self.min_duration,
                self.max_distance_km,
                self.min_unique_vessels,
                fishing_vessel_list,
            )
            | "EncodeAnchorages" >> beam.Map(encode_anchorage)
        )
