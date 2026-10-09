from __future__ import annotations
from dataclasses import dataclass

from gfw.common.config import DatePipelineConfig


@dataclass(frozen=True, kw_only=True)
class PortStateTransitionsConfig(DatePipelineConfig):
    bq_in_named_anchorages: str
    bq_in_messages: str
    bq_out_port_state_transitions: str

    ssvid_filter: str = None
    anchorage_entry_dist_km: float = 3.0
    anchorage_exit_dist_km: float = 4.0
    stopping_speed_knots: float = 0.2
    starting_speed_knots: float = 0.5
    min_anchorage_gap_minutes: float = 240.0
