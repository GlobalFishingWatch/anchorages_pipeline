import apache_beam as beam

from pipe_anchorages.core.named_anchorage_point import NamedAnchoragePoint

from .add_names_to_anchorages import AddNamesToAnchorages
from .create_override_anchorages import CreateOverrideAnchorages
from .find_used_s2ids import FindUsedS2ids


# No type hints on the functions Beam maps: it would infer coders from them.
def named_anchorage_to_msg(anchorage):
    """Converts a NamedAnchoragePoint into a row of the output table."""
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
        "label": anchorage.label,
        "sublabel": anchorage.sublabel,
        "label_source": anchorage.label_source,
        "iso3": anchorage.iso3,
    }


def is_not_removed(anchorage):
    """False for the anchorages the overrides remove (label REMOVE)."""
    return not (anchorage.label == "REMOVE" and anchorage.label_source == "anchorage_overrides")


class NameAnchorages(beam.PTransform):
    """Turns anchorage locations (query rows) into named anchorages table records (row dicts).

    Groups the inline chain (NamedAnchoragePoint.from_msg -> AddNamesToAnchorages, plus the
    anchorages CreateOverrideAnchorages adds -> drop the removed ones -> named_anchorage_to_msg)
    into one composite PTransform, since LinearDag's core slot takes exactly one transform.

    Constructor params are the raw config fields, not a config object.
    """

    def __init__(
        self,
        shapefile,
        label_distance_km,
        sublabel_distance_km,
        anchorage_overrides,
        reference_ports,
        reference_places,
    ) -> None:
        super().__init__()
        self.shapefile = shapefile
        self.label_distance_km = label_distance_km
        self.sublabel_distance_km = sublabel_distance_km
        self.anchorage_overrides = anchorage_overrides
        self.reference_ports = reference_ports
        self.reference_places = reference_places

    def expand(self, xs):
        port_info_config = {
            "override_path": self.anchorage_overrides,
            "port_list_paths": list(self.reference_ports) + list(self.reference_places),
            "label_distance_km": self.label_distance_km,
            "sublabel_distance_km": self.sublabel_distance_km,
        }

        existing_anchorages = (
            xs
            | "FromMsg" >> beam.Map(NamedAnchoragePoint.from_msg)
            | AddNamesToAnchorages(self.shapefile, port_info_config)
        )

        used_s2ids = beam.pvalue.AsList(
            existing_anchorages | FindUsedS2ids(self.anchorage_overrides)
        )

        new_anchorages = xs.pipeline | CreateOverrideAnchorages(
            self.anchorage_overrides, used_s2ids
        )

        return (
            (existing_anchorages, new_anchorages)
            | beam.Flatten()
            | "DropRemoved" >> beam.Filter(is_not_removed)
            | "ToMsg" >> beam.Map(named_anchorage_to_msg)
        )
