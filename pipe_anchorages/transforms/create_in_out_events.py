from __future__ import absolute_import, division, print_function

import datetime
from collections import namedtuple
from datetime import timedelta

import apache_beam as beam
import pytz
from pipe_anchorages import common as cmn
from pipe_anchorages.distance import distance, inf
from pipe_anchorages.objects.visit_event import VisitEvent

PseudoRcd = namedtuple("PseudoRcd", ["location", "timestamp"])


class InOutEventsBase:
    IN_PORT = "IN_PORT"
    AT_SEA = "AT_SEA"
    STOPPED = "STOPPED"

    in_port_states = (IN_PORT, STOPPED)
    all_states = (IN_PORT, AT_SEA, STOPPED)

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


class CreateInOutEvents(beam.PTransform, InOutEventsBase):
    def __init__(
        self,
        anchorages,
        anchorage_entry_dist,
        anchorage_exit_dist,
        stopped_begin_speed,
        stopped_end_speed,
        min_gap_minutes,
        end_date,
    ):
        self.anchorages = anchorages
        self.anchorage_entry_dist = anchorage_entry_dist
        self.anchorage_exit_dist = anchorage_exit_dist
        self.stopped_begin_speed = stopped_begin_speed
        self.stopped_end_speed = stopped_end_speed
        self.min_gap = timedelta(minutes=min_gap_minutes)
        self.end_date = end_date
        assert self.min_gap < timedelta(
            days=1
        ), "min gap must be under one day in current implementation"

    def _build_event(self, active_port, rcd, identity, event_type, last_timestamp):
        ssvid, vessel_id, seg_id = identity
        return VisitEvent(
            anchorage_id=active_port.s2id,
            lat=active_port.mean_location.lat,
            lon=active_port.mean_location.lon,
            vessel_lat=rcd.location.lat,
            vessel_lon=rcd.location.lon,
            ssvid=ssvid,
            seg_id=seg_id,
            vessel_id=vessel_id,
            timestamp=rcd.timestamp,
            event_type=event_type,
            last_timestamp=last_timestamp,
        )

    def _possibly_yield_gap_beg(
        self, seg_id, last_timestamp, last_state, next_timestamp, active_port
    ):
        if next_timestamp - last_timestamp >= self.min_gap:
            if last_state in self.in_port_states:
                evt_timestamp = last_timestamp + self.min_gap
                assert evt_timestamp <= next_timestamp
                rcd = PseudoRcd(
                    location=cmn.LatLon(None, None), timestamp=evt_timestamp
                )
                yield self._build_event(
                    active_port, rcd, seg_id, self.EVT_GAP_BEG, last_timestamp
                )

    def _create_in_out_events(self, grouped_records, anchorage_map):
        seg_id, records = grouped_records
        records = sorted(records, key=lambda x: x.timestamp)
        identity = records[0].identifier
        rcd = None
        last_state = None
        active_port = None
        last_timestamp = None
        for rcd in records:

            s2id = rcd.location.S2CellId(cmn.VISITS_S2_SCALE).to_token()
            port, dist = self._anchorage_distance(
                rcd.location, anchorage_map.get(s2id, [])
            )
            is_in_port = self._is_in_port(last_state, dist)
            active_port = port if is_in_port else active_port
            is_stopped = self._is_stopped(last_state, rcd.speed)
            state = self._compute_state(is_in_port, is_stopped)

            if last_timestamp is not None:
                if (
                    last_state in self.in_port_states
                    and rcd.timestamp - last_timestamp >= self.min_gap
                ):
                    yield self._build_event(
                        active_port, rcd, identity, self.EVT_GAP_END, last_timestamp
                    )
                yield from self._possibly_yield_gap_beg(
                    identity, last_timestamp, last_state, rcd.timestamp, active_port
                )

            for event_type in self.transition_map[(last_state, state)]:
                yield self._build_event(
                    active_port, rcd, identity, event_type, last_timestamp
                )

            last_timestamp = rcd.timestamp
            last_state = state

        end_time = datetime.datetime.combine(
            self.end_date, datetime.time.max, tzinfo=pytz.utc
        )
        yield from self._possibly_yield_gap_beg(
            identity, last_timestamp, last_state, end_time, active_port
        )

    def create_in_out_events(self, grouped_records, anchorage_map):
        identity, records = grouped_records
        return identity, list(
            self._create_in_out_events(grouped_records, anchorage_map)
        )

    def expand(self, grouped_records):
        anchorage_map = beam.pvalue.AsDict(self.anchorages)
        return grouped_records | beam.Map(
            self.create_in_out_events, anchorage_map=anchorage_map
        )
