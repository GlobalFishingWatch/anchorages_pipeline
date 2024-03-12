from __future__ import absolute_import, division, print_function

import datetime
from datetime import timedelta
from typing import NamedTuple

import apache_beam as beam
from pipe_anchorages import common as cmn

from ..common import LatLon
from .create_in_out_events import InOutEventsBase


class VisitLocationRecord:
    identifier: str
    timestamp: datetime
    location: LatLon
    speed: float
    is_possible_gap_end: bool


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
            rcd = VisitLocationRecord(
                identifier=rcd.identifier,
                timestamp=rcd.timestamp,
                location=rcd.location,
                speed=rcd.speed,
                is_possible_gap_end=False,
            )
            s2id = rcd.location.S2CellId(cmn.VISITS_S2_SCALE).to_token()
            port, dist = self._anchorage_distance(
                rcd.location, anchorage_map.get(s2id, [])
            )
            is_in_port = self._is_in_port(last_state, dist)
            active_port = port if is_in_port else active_port
            is_stopped = self._is_stopped(last_state, rcd.speed)
            state = self._compute_state(is_in_port, is_stopped)

            if i in (0, last_ndx):
                active.add(rcd)

            if (
                last_rcd is not None
                and rcd.timestamp - last_rcd.timestamp >= self.min_gap
            ):
                active.add(last_rcd)
                active.add(rcd._replace(is_possible_gap_end=True))

            if self.transition_map[(last_state, state)]:
                active.add(last_rcd)
                active.add(rcd)

            last_rcd = rcd
            last_state = state

        active = [x for x in active if x is not None]
        return sorted(active, key=lambda x: x.timestamp)

    def expand(self, grouped_records):
        anchorage_map = beam.pvalue.AsDict(self.anchorages)
        return grouped_records | beam.FlatMap(self.thin, anchorage_map=anchorage_map)
