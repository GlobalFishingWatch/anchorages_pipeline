from __future__ import absolute_import, division, print_function

from collections import namedtuple
from datetime import timedelta

import apache_beam as beam
from pipe_anchorages import common as cmn
from pipe_anchorages.distance import distance, inf
from pipe_anchorages.objects.visit_event import VisitEvent

PseudoRcd = namedtuple("PseudoRcd", ["location", "timestamp", "identifier"])


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
        if dist is None:
            return False
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
        anchorage_entry_dist,
        anchorage_exit_dist,
        stopped_begin_speed,
        stopped_end_speed,
        min_gap_minutes,
        end_date,
    ):
        self.anchorage_entry_dist = anchorage_entry_dist
        self.anchorage_exit_dist = anchorage_exit_dist
        self.stopped_begin_speed = stopped_begin_speed
        self.stopped_end_speed = stopped_end_speed
        self.min_gap = timedelta(minutes=min_gap_minutes)
        self.end_date = end_date
        self.last_possible_timestamp = end_date + timedelta(days=1) - timedelta(microseconds=1)
        assert self.min_gap < timedelta(
            days=1
        ), "min gap must be under one day in current implementation"

    def _build_event(self, active_port_rcd, rcd, event_type, last_timestamp):
        ssvid, vessel_id, seg_id = rcd.identifier
        return VisitEvent(
            anchorage_id=active_port_rcd.port_s2id,
            lat=active_port_rcd.port_lat,
            lon=active_port_rcd.port_lon,
            vessel_lat=rcd.location.lat,
            vessel_lon=rcd.location.lon,
            ssvid=ssvid,
            seg_id=seg_id,
            vessel_id=vessel_id,
            timestamp=rcd.timestamp,
            event_type=event_type,
            last_timestamp=last_timestamp,
        )

    def _yield_gap_beg(self, gap_end_rcd, last_timestamp, active_port_rcd):
        evt_timestamp = last_timestamp + self.min_gap
        assert evt_timestamp <= gap_end_rcd.timestamp
        rcd = PseudoRcd(
            location=cmn.LatLon(None, None),
            timestamp=evt_timestamp,
            identifier=gap_end_rcd.identifier,
        )
        yield self._build_event(active_port_rcd, rcd, self.EVT_GAP_BEG, last_timestamp)

    def _create_in_out_events(self, records):
        records = sorted(records, key=lambda x: x.timestamp)
        rcd = None
        last_state = None
        active_port_rcd = None
        last_timestamp = None
        for rcd in records:
            is_in_port = self._is_in_port(last_state, rcd.port_dist)
            active_port_rcd = rcd if is_in_port else active_port_rcd
            is_stopped = self._is_stopped(last_state, rcd.speed)
            state = self._compute_state(is_in_port, is_stopped)

            if last_timestamp is not None:
                if (
                    last_state in self.in_port_states
                    and rcd.is_possible_gap_end
                    and rcd.timestamp - last_timestamp >= self.min_gap
                ):
                    yield self._build_event(active_port_rcd, rcd, self.EVT_GAP_END, last_timestamp)
                    yield from self._yield_gap_beg(rcd, last_timestamp, active_port_rcd)

            for event_type in self.transition_map[(last_state, state)]:
                yield self._build_event(active_port_rcd, rcd, event_type, last_timestamp)

            last_timestamp = rcd.timestamp
            last_state = state
        if (
            # Trigger gap ends for gaps that started but we haven't reached their end
            last_state in self.in_port_states
            and self.last_possible_timestamp - last_timestamp >= self.min_gap
        ):
            psuedo_rcd = rcd._replace(timestamp=self.last_possible_timestamp)
            yield from self._yield_gap_beg(psuedo_rcd, last_timestamp, active_port_rcd)

    def create_in_out_events(self, grouped_records):
        identity, records = grouped_records
        return identity, list(self._create_in_out_events(records))

    def expand(self, grouped_records):
        return grouped_records | beam.Map(self.create_in_out_events)
