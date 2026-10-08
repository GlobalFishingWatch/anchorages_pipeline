import textwrap
from dataclasses import dataclass

from pipe_anchorages.assets import schemas

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


def collapse_paragraphs(text: str) -> str:
    """Collapse paragraphs with arbitrary newlines ('\n') into single lines."""
    paragraphs = textwrap.dedent(text).strip().split("\n\n")  # preserve paragraphs.
    cleaned = [" ".join(p.split()) for p in paragraphs]
    return "\n\n".join(cleaned)


SUMMARY = """\
Port visits, one row per visit of a vessel to a port. Each visit groups the port
events (entry, stop begin/end, gap begin/end and exit) found on the vessel's
port state transitions, joined to its vessel_id.
"""

CAVEATS = """\
⬖ To be completed.
"""


@dataclass
class PortVisitsTableDescription(TableDescription):
    repo_name: str = "pipe-anchorages"
    title: str = "PORT VISITS"
    subtitle: str = "Vessel visits to ports, built from port entry/stop/gap/exit events"
    summary: str = collapse_paragraphs(SUMMARY)
    caveats: str = CAVEATS


@dataclass
class PortVisitsTableConfig(TableConfig):
    schema_file: str = "port_visits.json"
    # Same layout as the legacy pipeline's output table.
    partition_type: str = "MONTH"
    partition_field: str = "end_timestamp"
    clustering_fields: tuple[str, ...] = ("end_timestamp",)

    @property
    def schema(self) -> list[dict]:
        return schemas.get_schema(self.schema_file)
