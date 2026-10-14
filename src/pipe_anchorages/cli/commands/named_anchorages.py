from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option

from pipe_anchorages.pipelines.named_anchorages.main import run


DESCRIPTION = """\
Assigns human-readable names to anchorage locations by looking up the nearest
known place (an anchorage-specific manual correction first, then a reference
list of real ports, then a broader reference list of any named place) within
a configurable distance.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_IN_LOCATIONS = "BigQuery table with unnamed anchorage locations to assign names to."
HELP_OUT_NAMED = "BigQuery table in which to store the named anchorages."
HELP_SHAPEFILE = "Path to the shapefile used to assign a country (iso3) to each anchorage."
HELP_LABEL_DIST_KM = (
    "Max distance (km) from a reference place for an anchorage to inherit its label."
)
HELP_SUBLABEL_DIST_KM = (
    "Max distance (km) from a reference place for an anchorage to also inherit its sublabel."
)
HELP_ANCHORAGE_OVERRIDES = (
    "Path to a file of manually verified anchorage name corrections, keyed by anchorage s2id. "
    "Takes priority over --reference-ports/--reference-places, and can also force-remove "
    "(label='REMOVE') or force-include a specific anchorage."
)
HELP_REFERENCE_PORTS = (
    "Paths to reference lists of real ports, checked (in order) before --reference-places."
)
HELP_REFERENCE_PLACES = (
    "Paths to reference lists of any named place, used as a fallback when no reference port "
    "is close enough."
)


class NamedAnchorages(Command):

    @property
    def name(self):
        return "named-anchorages"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option("--bq-in-anchorage-locations", type=str, required=True, help=HELP_IN_LOCATIONS),
            Option("--bq-out-named-anchorages", type=str, required=True, help=HELP_OUT_NAMED),
            Option("--shapefile", type=str, default="EEZ_Land_v3_202030.shp", help=HELP_SHAPEFILE),
            Option("--label-distance-km", type=float, default=4.0, help=HELP_LABEL_DIST_KM),
            Option("--sublabel-distance-km", type=float, default=1.0, help=HELP_SUBLABEL_DIST_KM),
            Option(
                "--anchorage-overrides",
                type=str,
                default="anchorage_overrides.csv",
                help=HELP_ANCHORAGE_OVERRIDES,
            ),
            Option(
                "--reference-ports",
                type=str,
                nargs="*",
                default=["peru.csv", "indonesia.csv", "WPI_ports.csv"],
                help=HELP_REFERENCE_PORTS,
            ),
            Option(
                "--reference-places",
                type=str,
                nargs="*",
                default=["geonames_1000.csv"],
                help=HELP_REFERENCE_PLACES,
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
