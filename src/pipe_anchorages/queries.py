import dataclasses
import datetime
from dataclasses import dataclass

from gfw.common.query import Query


@dataclass
class MessagesQuery(Query):
    """Position messages in [start_date, end_date), one row per position.

    Rows are keyed by `ident`, which holds the `ident_field` column (e.g. ssvid or seg_id).
    The destination column is left out when `include_destination` is False.
    Optionally limited to the vessels `ssvid_filter` (a subquery or a list of ssvids) returns.
    Its fields are the template's variables (see template_vars).
    """

    source_messages: str
    start_date: datetime.date
    end_date: datetime.date
    ident_field: str = "ssvid"
    include_destination: bool = True
    ssvid_filter: str = None

    template_filename = "messages.sql.j2"

    @property
    def template_vars(self) -> dict:
        return dataclasses.asdict(self)
