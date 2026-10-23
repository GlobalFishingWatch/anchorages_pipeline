import apache_beam as beam

from pipe_anchorages.core.namedtuples import _datetime_to_s
from pipe_anchorages.core.visit_event import VisitEvent
from pipe_anchorages.core.visit_location_record import VisitLocationRecord
from pipe_anchorages.transforms.shrink_group_by import ShrinkGroupBy

from .create_in_out_events import CreateInOutEvents
from .create_port_visits import CreatePortVisits


def event_to_msg(x: VisitEvent) -> dict:
    """Converts a VisitEvent into a row of the output table's `events` field."""
    x = x._asdict()
    x["timestamp"] = _datetime_to_s(x["timestamp"])
    x.pop("vessel_id")
    x.pop("last_timestamp")
    x.pop("ssvid")
    return x


# No type hints on the functions Beam maps: it would infer coders from them, and the
# registered PortVisitCoder can't encode a visit's events.
def visit_to_msg(x):
    """Converts a PortVisit into a row of the output table."""
    x = x._asdict()
    x["events"] = [event_to_msg(y) for y in x["events"]]
    x["start_timestamp"] = _datetime_to_s(x["start_timestamp"])
    x["end_timestamp"] = _datetime_to_s(x["end_timestamp"])
    return x


class DetectPortVisits(beam.PTransform):
    """Turns port state transitions (query rows) into port visit table records (row dicts).

    Groups the inline chain (VisitLocationRecord.from_msg -> ShrinkGroupBy ->
    CreateInOutEvents -> CreatePortVisits -> visit_to_msg) into one composite PTransform, since
    LinearDag's core slot takes exactly one transform.

    Constructor params are the raw config fields, not a config object.
    """

    def __init__(
        self,
        anchorage_entry_dist_km,
        anchorage_exit_dist_km,
        stopping_speed_knots,
        starting_speed_knots,
        min_anchorage_gap_minutes,
        max_inter_seg_dist_nm,
        end_time,
    ) -> None:
        super().__init__()
        self.anchorage_entry_dist_km = anchorage_entry_dist_km
        self.anchorage_exit_dist_km = anchorage_exit_dist_km
        self.stopping_speed_knots = stopping_speed_knots
        self.starting_speed_knots = starting_speed_knots
        self.min_anchorage_gap_minutes = min_anchorage_gap_minutes
        self.max_inter_seg_dist_nm = max_inter_seg_dist_nm
        self.end_time = end_time

    def expand(self, xs):
        return (
            xs
            | "FromMsg" >> beam.Map(VisitLocationRecord.from_msg)
            | ShrinkGroupBy(
                "ssvid", "vessel_id", record_type=VisitLocationRecord, elements="Transitions",
            )
            | CreateInOutEvents(
                anchorage_entry_dist=self.anchorage_entry_dist_km,
                anchorage_exit_dist=self.anchorage_exit_dist_km,
                stopped_begin_speed=self.stopping_speed_knots,
                stopped_end_speed=self.starting_speed_knots,
                min_gap_minutes=self.min_anchorage_gap_minutes,
                end_time=self.end_time,
            )
            | CreatePortVisits(self.max_inter_seg_dist_nm)
            | "VisitToMsg" >> beam.Map(visit_to_msg)
        )
