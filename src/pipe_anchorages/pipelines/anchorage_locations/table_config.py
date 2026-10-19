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
Candidate anchorage points, found by clustering positions where vessels stay
stationary for extended periods. Each point aggregates how many unique
vessels visited, was it a stationary fishing SSVID, and the RMS drift radius.
"""

CAVEATS = """\
⬖ To be completed.
"""


@dataclass
class AnchorageLocationsTableDescription(TableDescription):
    repo_name: str = "pipe-anchorages"
    title: str = "ANCHORAGE POINTS"
    subtitle: str = "Candidate anchorage locations derived from vessel stationary detections"
    summary: str = collapse_paragraphs(SUMMARY)
    caveats: str = CAVEATS


@dataclass
class AnchorageLocationsTableConfig(TableConfig):
    schema_file: str = "anchorage_locations.json"

    @property
    def schema(self) -> list[dict]:
        return schemas.get_schema(self.schema_file)
