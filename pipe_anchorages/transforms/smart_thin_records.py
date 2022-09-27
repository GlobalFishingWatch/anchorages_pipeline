from __future__ import absolute_import, division, print_function

import datetime
from collections import namedtuple
from datetime import timedelta

import apache_beam as beam
from pipe_anchorages import common as cmn
from pipe_anchorages.distance import distance, inf

PseudoRcd = namedtuple("PseudoRcd", ["location", "timestamp"])


class SmartThinRecords(beam.PTransform):

    IN_PORT = "IN_PORT"
    AT_SEA = "AT_SEA"
    STOPPED = "STOPPED"

    in_port_states = (IN_PORT, STOPPED)
    all_states = (IN_PORT, AT_SEA, STOPPED)

    EVT_INITIAL = "INITIAL_STATE"
    EVT_FINAL = "FINAL_STATE"
    EVT_ENTER = "PORT_ENTRY"
    EVT_EXIT = "PORT_EXIT"
    EVT_STOP = "PORT_STOP_BEGIN"
    EVT_START = "PORT_STOP_END"
    EVT_GAP_BEG = "PORT_GAP_BEGIN"
    EVT_GAP_END = "PORT_GAP_END"

    transition_map = {
        (AT_SEA, AT_SEA): [],
        (AT_SEA, IN_PORT): [EVT_ENTER],
        (AT_SEA, STOPPED): [EVT_ENTER, EVT_STOP],
        (IN_PORT, AT_SEA): [EVT_EXIT],
        (IN_PORT, IN_PORT): [],
        (IN_PORT, STOPPED): [EVT_STOP],
        (STOPPED, AT_SEA): [EVT_START, EVT_EXIT],
        (STOPPED, IN_PORT): [EVT_START],
        (STOPPED, STOPPED): [],
        (None, AT_SEA): [],
        (None, IN_PORT): [],
        (None, STOPPED): [],
    }

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
        # TODO: use dates for start and end date
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

    def _is_in_port(self, state, dist):
        if dist <= self.anchorage_entry_dist:
            return True
        elif dist >= self.anchorage_exit_dist:
            return False
        else:
            return state in (self.IN_PORT, self.STOPPED)

    def _is_stopped(self, state, speed):
        if speed <= self.stopped_begin_speed:
            return True
        elif speed >= self.stopped_end_speed:
            return False
        else:
            return state == self.STOPPED

    def _anchorage_distance(self, loc, anchorages):
        closest = None
        min_dist = inf
        for anch in sorted(anchorages, key=lambda x: x.s2id):
            dist = distance(loc, anch.mean_location)
            if dist < min_dist:
                min_dist = dist
                closest = anch
        return closest, min_dist

    def _compute_state(self, is_in_port, is_stopped):
        if is_in_port:
            if is_stopped:
                return self.STOPPED
            else:
                return self.IN_PORT
        else:
            return self.AT_SEA

    def thin(self, grouped_records, anchorage_map):
        (seg_id, date), records = grouped_records

        last_ndx = len(records) - 1
        active = set()
        last_rcd = None
        last_state = None
        active_port = None
        for i, rcd in enumerate(records):
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
                active.add(rcd)

            # TODO: we should grab transition map from create_in_out_events
            # TODO: perhaps even share more implementation

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
