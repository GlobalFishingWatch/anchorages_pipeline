"""This module implements a CLI for the anchorages pipeline."""
import sys
import logging

from gfw.common.logging import LoggerConfig
from gfw.common.cli import CLI, labels_option
from gfw.common.cli.formatting import default_formatter

from pipe_anchorages.version import __version__
from pipe_anchorages.cli.commands import (
    AnchorageLocations,
    AnchoragesVisitedInfo,
    ConfidenceVoyages,
    NamedAnchorages,
    PortStateTransitions,
    PortVisits,
)


logger = logging.getLogger(__name__)


NAME = "pipe-anchorages"
DESCRIPTION = "Tools for finding anchorages and associated port-visit events."


def run(args):
    cli = CLI(
        name=NAME,
        description=DESCRIPTION,
        formatter=default_formatter(max_pos=120),
        subcommands=[
            AnchorageLocations,
            AnchoragesVisitedInfo,
            ConfidenceVoyages,
            NamedAnchorages,
            PortStateTransitions,
            PortVisits,
        ],
        options=[labels_option()],  # Common options for all subcommands.
        version=__version__,
        examples=[
            "pipe-anchorages anchorages-visited-info -c config/sample-anchorages-visited.json "
            "--labels environment=development resource_creator=tomas-link "
            "project=ais stage=anchorages",
            "pipe-anchorages confidence-voyages "
            "--bq-in-port-visits project.dataset.port_visits --min-confidence 3 "
            "--bq-out-voyages project.dataset.voyages_c3 --project world-fishing-827 "
            "--labels environment=development resource_creator=tomas-link "
            "project=ais stage=anchorages",
            "pipe-anchorages port-state-transitions "
            "--bq-in-named-anchorages project.dataset.anchorages "
            "--bq-in-messages project.dataset.messages "
            "--bq-out-port-state-transitions project.dataset.output "
            "--start-date 2024-01-01 --end-date 2024-01-07 "
            "--labels environment=development resource_creator=tomas-link "
            "project=ais stage=anchorages",
            "pipe-anchorages port-visits "
            "--bq-in-port-events project.dataset.port_events "
            "--bq-in-segment-info project.dataset.segment_info "
            "--bq-out-port-visits project.dataset.port_visits "
            "--start-date 2024-01-01 --end-date 2024-01-07 "
            "--labels environment=development resource_creator=tomas-link "
            "project=ais stage=anchorages",
            "pipe-anchorages anchorage-locations "
            "--bq-in-messages project.dataset.messages "
            "--bq-out-anchorage-locations project.dataset.anchorage_locations "
            "--start-date 2024-01-01 --end-date 2024-01-07 "
            "--gcs-in-fishing-ssvids gs://bucket/fishing_mmsi.txt "
            "--labels environment=development resource_creator=tomas-link "
            "project=ais stage=anchorages",
            "pipe-anchorages named-anchorages "
            "--bq-in-anchorage-locations project.dataset.anchorage_locations "
            "--bq-out-named-anchorages project.dataset.named_anchorages "
            "--labels environment=development resource_creator=tomas-link "
            "project=ais stage=anchorages",
        ],
        logger_config=LoggerConfig(
            warning_level=[
                "apache_beam.runners.portability",
                "apache_beam.runners.worker",
                "apache_beam.transforms.core",
                "apache_beam.io.filesystem",
                "apache_beam.io.gcp.bigquery_tools",
                "urllib3"
            ]
        ),
        allow_unknown=True
    )

    return cli.execute(args)


def main():
    run(sys.argv[1:])


if __name__ == "__main__":
    main()
