from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option

from pipe_anchorages.cli.commands.port_state_transitions import PortStateTransitions
from pipe_anchorages.pipelines.port_visits.main import run


DESCRIPTION = """\
Detects port visits by joining port state transitions with vessel identity,
building entry/exit events per vessel and grouping them into visits.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_MESSAGES = "BigQuery table with the port-state-transitions output."
HELP_IN_SEGMENT_INFO = "BigQuery table mapping vessel_id to seg_id, one vessel_id per seg_id."
HELP_OUT_PORT_VISITS = "BigQuery table in which to store the port visits."
HELP_START_DATE = "First date (inclusive) to generate visits."
HELP_END_DATE = "End date (exclusive) to generate visits."
HELP_BAD_SEGS = "Subquery producing segment ids of bad segments to exclude."
HELP_INTERSEG_DIST = (
    "Segments more than this distance apart will not be joined when creating visits."
)


class PortVisits(Command):

    @property
    def name(self):
        return "port-visits"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option("--bq-in-port-state-transitions", type=str, required=True, help=HELP_MESSAGES),
            Option("--bq-in-segment-info", type=str, required=True, help=HELP_IN_SEGMENT_INFO),
            Option("--bq-out-port-visits", type=str, required=True, help=HELP_OUT_PORT_VISITS),
            Option("--start-date", type=str, required=True, help=HELP_START_DATE),
            Option("--end-date", type=str, required=True, help=HELP_END_DATE),
            Option("--bad-segs", type=str, help=HELP_BAD_SEGS),
            Option("--max-inter-seg-dist-nm", type=float, default=60.0, help=HELP_INTERSEG_DIST),
            *PortStateTransitions.transition_options(),
            Option(
                "--mock-bq-clients",
                type=bool,
                help="If passed, mocks the BQ clients [Useful for development].",
            ),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return run(config, **kwargs)
