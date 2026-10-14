from importlib.resources import files

import apache_beam as beam

from pipe_anchorages.assets.data import EEZ
from pipe_anchorages.core.named_anchorage_location import NamedAnchorageLocation
from pipe_anchorages.nearest_port import Port
from pipe_anchorages.port_info_finder import PortInfoFinder
from pipe_anchorages.port_info_finder import normalize_label
from pipe_anchorages.shapefile_to_iso3 import get_iso3_finder


class AddNamesToAnchorages(beam.PTransform):
    """Sets the label, sublabel, iso3 and label_source of each NamedAnchorageLocation.

    The name comes from the nearest place in the lists of `config["port_list_paths"]`, after the
    anchorage overrides (see PortInfoFinder): with its sublabel within
    `config["sublabel_distance_km"]`, without it within `config["label_distance_km"]`. Anchorages
    with no place that close are named after their top destination, or their s2id if they have
    none, with label_source "top_destination".

    The iso3 comes from that place, or else from the EEZ of the `shapefile_path` shapefile the
    anchorage is in. Anchorages in China get their s2id as label (label_source
    "china_s2id_override"), unless named by the overrides.

    `config` has the keys PortInfoFinder.from_config takes. The finders load their files lazily,
    on the workers.
    """

    _port_info_finder = None
    _iso3_finder = None

    def __init__(self, shapefile_path: str, config: dict) -> None:
        self.config = config
        self.shapefile_path = shapefile_path

    @property
    def port_info_finder(self) -> PortInfoFinder:
        """The PortInfoFinder of `config`, created on first use."""
        if self._port_info_finder is None:
            self._port_info_finder = PortInfoFinder.from_config(self.config)
        return self._port_info_finder

    # No type hints on the methods Beam maps: it would infer coders from them.
    def add_best_label(self, anchorage):
        """Returns `anchorage` with the label, sublabel, iso3 and label_source of its place."""
        port_info, source = self.port_info_finder.find(anchorage.mean_location)
        if port_info is None:
            port_info = Port(
                iso3="",
                label=anchorage.top_destination,
                sublabel="",
                lat=None,
                lon=None,
            )
            source = "top_destination"
        map = anchorage._asdict()
        map["label"] = normalize_label(port_info.label)
        if map["label"] in ("", None):
            map["label"] = map["s2id"]
        map["sublabel"] = normalize_label(port_info.sublabel)
        map["iso3"] = normalize_label(port_info.iso3)
        map["label_source"] = source
        return NamedAnchorageLocation(**map)

    def add_iso3(self, named_anchorage):
        """Returns `named_anchorage` with its EEZ's iso3 if it has none, and China's override."""
        if named_anchorage.iso3 is None:
            finder = get_iso3_finder(files(EEZ).joinpath(self.shapefile_path))
            iso3 = finder.iso3(
                named_anchorage.mean_location.lat, named_anchorage.mean_location.lon
            )
            if iso3 in (None, "-"):
                iso3 = None
        else:
            iso3 = named_anchorage.iso3
        # If the anchorage is in China, just use the s2id as the anchorage ID because
        # unless from anchorage overrides, because Chinese anchorage IDs are so unreliable.
        if iso3 == "CHN" and named_anchorage.label_source != "anchorage_overrides":
            named_anchorage = named_anchorage._replace(
                label=named_anchorage.s2id, label_source="china_s2id_override"
            )
        return named_anchorage._replace(iso3=iso3)

    def expand(self, anchorages: beam.PCollection) -> beam.PCollection:
        return anchorages | beam.Map(self.add_best_label) | beam.Map(self.add_iso3)
