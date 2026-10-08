import datetime
import math

import apache_beam as beam

from pipe_anchorages import common as cmn
from pipe_anchorages.core.namedtuples import _datetime_to_s
from pipe_anchorages.transforms.create_in_out_events import CreateInOutEvents
from pipe_anchorages.transforms.create_port_visits import CreatePortVisits
from pipe_anchorages.transforms.smart_thin_records import VisitLocationRecord


def from_msg(x):
    x_new = x.copy()
    x_new["timestamp"] = datetime.datetime.fromtimestamp(x_new["timestamp"], datetime.UTC)
    ssvid = x_new.pop("ssvid")
    seg_id = x_new.pop("seg_id")
    vessel_id = x_new.pop("vessel_id")
    ident = (ssvid, vessel_id, seg_id)
    loc = cmn.LatLon(x_new.pop("lat"), x_new.pop("lon"))
    port_dist = x_new.pop("port_dist")
    if port_dist is None:
        port_dist = math.inf
    return vessel_id, VisitLocationRecord(
        identifier=ident, location=loc, port_dist=port_dist, **x_new
    )


def event_to_msg(x):
    x = x._asdict()
    x["timestamp"] = _datetime_to_s(x["timestamp"])
    x.pop("vessel_id")
    x.pop("last_timestamp")
    x.pop("ssvid")
    return x


def visit_to_msg(x):
    x = x._asdict()
    x["events"] = [event_to_msg(y) for y in x["events"]]
    x["start_timestamp"] = _datetime_to_s(x["start_timestamp"])
    x["end_timestamp"] = _datetime_to_s(x["end_timestamp"])
    return x


class DetectPortVisits(beam.PTransform):
    """Turns port state transitions (query rows) into port visit table records (row dicts).

    Groups the inline chain (from_msg -> GroupByKey -> CreateInOutEvents -> CreatePortVisits ->
    visit_to_msg) into one composite PTransform, since LinearDag's core slot takes exactly one
    transform.

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
            | "FromMsg" >> beam.Map(from_msg)
            | "GroupByVesselId" >> beam.GroupByKey()
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
