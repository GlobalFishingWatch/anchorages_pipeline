from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option, date_range_options

from pipe_anchorages.pipelines.anchorage_locations.main import run


DESCRIPTION = """\
Finds candidate anchorage locations by clustering vessel positions where they
remain stationary near the coast.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_IN_MESSAGES = "BigQuery table to pull position messages from."
HELP_LOCATIONS = "BigQuery table in which to store the anchorage locations."
HELP_FISHING_SSVIDS = "Newline-separated list of fishing vessel ids."
HELP_MIN_POSITIONS = "Minimum number of positions a segment needs to be considered."
HELP_STATIONARY_PERIOD_MIN_DURATION_MINUTES = (
    "Minimum time (minutes) a vessel must stay within the stationary radius to count."
)
HELP_STATIONARY_PERIOD_MAX_DISTANCE_KM = (
    "Max drift radius (km) from a position while still considered stationary there."
)
HELP_MIN_UNIQUE_VESSELS = (
    "Minimum number of distinct vessels that must visit a cluster for it to count as an anchorage."
)


class AnchorageLocations(Command):

    @property
    def name(self):
        return "anchorage-locations"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option("--bq-in-messages", type=str, required=True, help=HELP_IN_MESSAGES),
            Option("--bq-out-anchorage-locations", type=str, required=True, help=HELP_LOCATIONS),
            Option("--gcs-in-fishing-ssvids", type=str, required=True, help=HELP_FISHING_SSVIDS),
            *date_range_options(),
            Option("--min-positions", type=int, default=200, help=HELP_MIN_POSITIONS),
            Option("--min-unique-vessels", type=int, default=20, help=HELP_MIN_UNIQUE_VESSELS),
            Option(
                "--stationary-period-min-duration-minutes",
                type=int,
                default=720,
                help=HELP_STATIONARY_PERIOD_MIN_DURATION_MINUTES,
            ),
            Option(
                "--stationary-period-max-distance-km",
                type=float,
                default=0.5,
                help=HELP_STATIONARY_PERIOD_MAX_DISTANCE_KM,
            ),
            Option(
                "--mock-bq-clients",
                type=bool,
                help="If passed, mocks the BQ clients [Useful for development].",
            ),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return run(config, **kwargs)
