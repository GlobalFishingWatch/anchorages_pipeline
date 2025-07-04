from __future__ import absolute_import, division, print_function

import datetime
import math
from datetime import timedelta
from typing import NamedTuple, Optional

import apache_beam as beam
from pipe_anchorages import common as cmn

from ..common import LatLon
from .create_in_out_events import InOutEventsBase


class VisitLocationRecord(NamedTuple):
    identifier: str
    timestamp: datetime.datetime
    location: LatLon
    speed: float
    is_possible_gap_end: bool
    port_s2id: Optional[str]
    port_dist: Optional[float]
    port_lon: Optional[float]
    port_lat: Optional[float]


class SmartThinRecords(beam.PTransform, InOutEventsBase):
    def __init__(
        self,
        anchorages,
        anchorage_entry_dist,
        anchorage_exit_dist,
        stopped_begin_speed,
        stopped_end_speed,
        min_gap_minutes,
        start_date,
        end_date,
    ):
        self.anchorages = anchorages
        self.anchorage_entry_dist = anchorage_entry_dist
        self.anchorage_exit_dist = anchorage_exit_dist
        self.stopped_begin_speed = stopped_begin_speed
        self.stopped_end_speed = stopped_end_speed
        self.min_gap = timedelta(minutes=min_gap_minutes)
        assert self.min_gap < timedelta(
            days=1
        ), "min gap must be under one day in current implementation"
        assert isinstance(start_date, datetime.date)
        self.start_date = start_date
        assert isinstance(end_date, datetime.date)
        self.end_date = end_date

    def thin(self, grouped_records, anchorage_map):
        # This mirrors the implementation of _create_in_out_events in
        # CreateInOutEvents and possibly we should try to combine them
        # at some point
        (seg_id, date), records = grouped_records

        last_ndx = len(records) - 1
        active = set()
        last_rcd = None
        last_state = None
        active_port = None
        for i, rcd in enumerate(records):
            s2id = rcd.location.S2CellId(cmn.VISITS_S2_SCALE).to_token()
            port, dist = self._anchorage_distance(rcd.location, anchorage_map.get(s2id, []))

            rcd = VisitLocationRecord(
                identifier=rcd.identifier,
                timestamp=rcd.timestamp,
                location=rcd.location,
                speed=rcd.speed,
                is_possible_gap_end=False,
                port_s2id=port.s2id if port else None,
                port_dist=dist if not math.isinf(dist) else None,
                port_lon=port.mean_location.lon if port else None,
                port_lat=port.mean_location.lat if port else None,
            )

            is_in_port = self._is_in_port(last_state, dist)
            active_port = port if is_in_port else active_port
            is_stopped = self._is_stopped(last_state, rcd.speed)
            state = self._compute_state(is_in_port, is_stopped)

            if i == 0:
                # Always mark first record of day as possible gap end since
                # could be arbitrarily long gap from previous days.
                # We update the record here so that we don't store twice
                # if this record is stored for other reasons
                rcd = rcd._replace(is_possible_gap_end=True)

            if last_rcd is not None and rcd.timestamp - last_rcd.timestamp >= self.min_gap:
                # This record looks like the end of a gap based on this segment.
                # We again, update the record here so that we don't store twice
                # if this record is stored for other reasons
                rcd = rcd._replace(is_possible_gap_end=True)
                active.add(last_rcd)
                active.add(rcd)

            if i in (0, last_ndx):
                # Always store first and last record of day so we correctly deal
                # with gaps across the day boundary.
                active.add(rcd)

            if self.transition_map[(last_state, state)]:
                # Store points surrounding any transition.
                active.add(last_rcd)
                active.add(rcd)

            last_rcd = rcd
            last_state = state

        active = [x for x in active if x is not None]
        return sorted(active, key=lambda x: x.timestamp)

    def expand(self, grouped_records):
        anchorage_map = beam.pvalue.AsDict(self.anchorages)
        return grouped_records | beam.FlatMap(self.thin, anchorage_map=anchorage_map)
