from __future__ import annotations
from dataclasses import dataclass

from gfw.common.config import PipelineConfig


@dataclass(frozen=True, kw_only=True)
class PortVisitsConfig(PipelineConfig):
    bq_in_port_state_transitions: str
    bq_in_segment_info: str
    bq_out_port_visits: str

    # Defaults must be declared (they shadow PipelineConfig's own start_date/end_date
    # properties, which read from date_range) -- see AnchorageLocationsConfig.
    # TODO: switch the CLI to a single --date-range option and drop these fields.
    start_date: str = None
    end_date: str = None

    bad_segs: str = None
    max_inter_seg_dist_nm: float = 60.0
    anchorage_entry_dist_km: float = 3.0
    anchorage_exit_dist_km: float = 4.0
    stopping_speed_knots: float = 0.2
    starting_speed_knots: float = 0.5
    min_anchorage_gap_minutes: float = 240.0

    # date_range is declared in the base class as required; see start_date above.
    date_range: tuple[str, str] = None
