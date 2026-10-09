from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option, date_range_options

from pipe_anchorages.pipelines.port_state_transitions.main import run


DESCRIPTION = """\
Selects, from all position messages in the date range, the subset that
port-visits needs to detect port visits. A position is kept if it is:

  * on either side of a candidate port state transition: entering or exiting
    port, or stopping or starting to move while in port, where "in port" means
    within a distance threshold of the nearest named anchorage;
  * on either side of a tracking gap;
  * the first or last position of a segment-day.

Each output row is one of those positions (a position message enriched with
the nearest anchorage), not a transition or an event: transitions are implied
by consecutive rows, and this is an intermediate step, not a finished product.

This command processes positions in bounded windows (currently per segment,
per day), so it can't always tell on its own whether a transition or a gap
happened at the edge of its window -- it conservatively keeps the boundary
positions either way. port-visits, which groups each vessel's positions across
all its segments and days in the processed range, resolves those cases and
builds the actual port events (PORT_ENTRY, PORT_EXIT, ...) and visits.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_IN_ANCHORAGES = "BigQuery table with named anchorages."
HELP_IN_MESSAGES = "BigQuery table to pull position messages from."
HELP_OUT_POSITIONS = "BigQuery table in which to store the selected position messages."
HELP_SSVID_FILTER = (
    "Subquery or list of ssvid to limit processing to. If prefixed by @, load from given path."
)
HELP_ENTRY_DIST_KM = "Max distance (km) from an anchorage to count as an entry."
HELP_EXIT_DIST_KM = "Min distance (km) from an anchorage to count as an exit."
HELP_STOPPING_KNOTS = "Speed (knots) below which a vessel is considered stopped."
HELP_STARTING_KNOTS = "Speed (knots) above which a stopped vessel is moving again."
HELP_MIN_GAP = (
    "Minimum gap (minutes) between two records for them to count as separate anchorage visits."
)


class PortStateTransitions(Command):

    @property
    def name(self):
        return "port-state-transitions"

    @property
    def description(self):
        return DESCRIPTION

    @staticmethod
    def transition_options() -> list:
        """Options shared with port-visits, which applies the same state machine.

        Owned here since this command runs first in the pipeline; port-visits
        imports this method rather than redeclaring the same 5 Options, so the
        two can never drift apart.
        """
        return [
            Option("--anchorage-entry-dist-km", type=float, default=3.0, help=HELP_ENTRY_DIST_KM),
            Option("--anchorage-exit-dist-km", type=float, default=4.0, help=HELP_EXIT_DIST_KM),
            Option("--stopping-speed-knots", type=float, default=0.2, help=HELP_STOPPING_KNOTS),
            Option("--starting-speed-knots", type=float, default=0.5, help=HELP_STARTING_KNOTS),
            Option("--min-anchorage-gap-minutes", type=float, default=240.0, help=HELP_MIN_GAP),
        ]

    @property
    def options(self):
        return [
            Option("--bq-in-named-anchorages", type=str, required=True, help=HELP_IN_ANCHORAGES),
            Option("--bq-in-messages", type=str, required=True, help=HELP_IN_MESSAGES),
            Option(
                "--bq-out-port-state-transitions", type=str, required=True, help=HELP_OUT_POSITIONS
            ),
            *date_range_options(),
            Option("--ssvid-filter", type=str, help=HELP_SSVID_FILTER),
            *self.transition_options(),
            Option(
                "--mock-bq-clients",
                type=bool,
                help="If passed, mocks the BQ clients [Useful for development].",
            ),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return run(config, **kwargs)
