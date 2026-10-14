from __future__ import annotations
from dataclasses import dataclass

from gfw.common.config import PipelineConfig


@dataclass(frozen=True, kw_only=True)
class NamedAnchoragesConfig(PipelineConfig):
    bq_in_anchorage_locations: str
    bq_out_named_anchorages: str

    shapefile: str = "EEZ_Land_v3_202030.shp"
    label_distance_km: float = 4.0
    sublabel_distance_km: float = 1.0
    anchorage_overrides: str = "anchorage_overrides.csv"
    reference_ports: tuple[str, ...] = ("peru.csv", "indonesia.csv", "WPI_ports.csv")
    reference_places: tuple[str, ...] = ("geonames_1000.csv",)
