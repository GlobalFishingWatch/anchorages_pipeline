from __future__ import annotations
from dataclasses import dataclass

from gfw.common.config import PipelineConfig


@dataclass(frozen=True, kw_only=True)
class AnchoragesVisitedInfoConfig(PipelineConfig):
    bq_input_loitering: str
    bq_input_encounters: str
    bq_input_ais_gaps: str
    bq_input_named_anchorages: str
    bq_output: str
    project: str = None
    dry_run: bool = False
