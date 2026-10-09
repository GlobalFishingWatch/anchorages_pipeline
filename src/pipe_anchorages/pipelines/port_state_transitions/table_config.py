import textwrap
from dataclasses import dataclass
from datetime import date
from typing import Optional

from pipe_anchorages.assets import schemas

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


def collapse_paragraphs(text: str) -> str:
    """Collapse paragraphs with arbitrary newlines ('\n') into single lines."""
    paragraphs = textwrap.dedent(text).strip().split("\n\n")  # preserve paragraphs.
    cleaned = [" ".join(p.split()) for p in paragraphs]
    return "\n\n".join(cleaned)


SUMMARY = """\
Position messages around candidate port state transitions, one row per position:
on either side of entering or exiting port, stopping or starting to move while in
port, or a tracking gap, plus the first and last position of each segment-day.
Each row carries the nearest anchorage. This is the input of port-visits, not a
finished product.
"""

CAVEATS = """\
⬖ To be completed.
"""


@dataclass
class PortStateTransitionsTableDescription(TableDescription):
    repo_name: str = "pipe-anchorages"
    title: str = "PORT STATE TRANSITIONS"
    subtitle: str = "Position messages around candidate port state transitions"
    summary: str = collapse_paragraphs(SUMMARY)
    caveats: str = CAVEATS


@dataclass
class PortStateTransitionsTableConfig(TableConfig):
    schema_file: str = "port_state_transitions.json"
    # Same layout as the legacy pipeline's output table.
    partition_type: str = "MONTH"
    partition_field: str = "timestamp"
    clustering_fields: tuple[str, ...] = ("timestamp", "is_possible_gap_end")

    @property
    def schema(self) -> list[dict]:
        return schemas.get_schema(self.schema_file)

    def delete_query(self, start_date: date, end_date: Optional[date] = None) -> str:
        """Returns the query that deletes the rows of the processed date range.

        The pipeline appends, so the range is cleared first to make reprocessing it
        idempotent. Without end_date, it deletes everything from start_date on.
        """
        query = f"DELETE FROM `{self.table_id}` WHERE DATE(timestamp) >= '{start_date}'"
        if end_date is not None:
            query += f" AND DATE(timestamp) <= '{end_date}'"

        return query
