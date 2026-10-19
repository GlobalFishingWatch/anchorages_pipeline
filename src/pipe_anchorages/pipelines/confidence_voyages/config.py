from __future__ import annotations
from dataclasses import dataclass

from gfw.common.config import PipelineConfig


@dataclass(frozen=True, kw_only=True)
class ConfidenceVoyagesConfig(PipelineConfig):
    bq_in_port_visits: str
    bq_out_voyages: str
    min_confidence: str
    project: str = None
