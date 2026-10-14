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
Anchorage locations with a port name (label), an anchorage name (sublabel) and the
ISO3 of the EEZ they are in. Names come from the nearest known place: a manual
correction for that anchorage first, then a reference list of real ports, then a
broader reference list of any named place, within a distance threshold. Anchorages
without a nearby place fall back to the vessels' most reported destination.
"""

CAVEATS = """\
⬖ To be completed.
"""


@dataclass
class NamedAnchoragesTableDescription(TableDescription):
    repo_name: str = "pipe-anchorages"
    title: str = "NAMED ANCHORAGES"
    subtitle: str = "Anchorage locations with port and anchorage names"
    summary: str = collapse_paragraphs(SUMMARY)
    caveats: str = CAVEATS


@dataclass
class NamedAnchoragesTableConfig(TableConfig):
    schema_file: str = "named_anchorages.json"

    @property
    def schema(self) -> list[dict]:
        return schemas.get_schema(self.schema_file)
