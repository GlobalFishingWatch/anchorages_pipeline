from __future__ import annotations
from dataclasses import dataclass

from gfw.common.config import DatePipelineConfig


@dataclass(frozen=True, kw_only=True)
class AnchorageLocationsConfig(DatePipelineConfig):
    bq_in_messages: str
    bq_out_anchorage_locations: str
    gcs_in_fishing_ssvids: str

    min_positions: int = 200
    min_unique_vessels: int = 20
    stationary_period_min_duration_minutes: int = 720
    stationary_period_max_distance_km: float = 0.5
