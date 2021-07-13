from __future__ import absolute_import, print_function, division

import datetime
from datetime import timedelta
from collections import namedtuple
import pytz
import apache_beam as beam
from apache_beam import pvalue

from pipe_anchorages import common as cmn
from pipe_anchorages.distance import distance, inf
from pipe_anchorages.objects.visit_event import VisitEvent
from pipe_anchorages.objects.namedtuples import s_to_datetime
import logging

PseudoRcd = namedtuple('PseudoRcd', ['location', 'timestamp'])

class CreateInOutEvents(beam.PTransform):

    IN_PORT = "IN_PORT"
    AT_SEA  = "AT_SEA"
    STOPPED = "STOPPED"

    in_port_states = (IN_PORT, STOPPED)
    all_states = (IN_PORT, AT_SEA, STOPPED)

    EVT_ENTER = 'PORT_ENTRY'
    EVT_EXIT  = 'PORT_EXIT'
    EVT_STOP  = 'PORT_STOP_BEGIN'
    EVT_START = 'PORT_STOP_END'
    EVT_GAP_BEG = 'PORT_GAP_BEGIN'
    EVT_GAP_END = 'PORT_GAP_END'

    transition_map = {
        (AT_SEA, AT_SEA)   : [],
        (AT_SEA, IN_PORT)  : [EVT_ENTER],
        (AT_SEA, STOPPED)  : [EVT_ENTER, EVT_STOP],
        (IN_PORT, AT_SEA)  : [EVT_EXIT],
        (IN_PORT, IN_PORT) : [],
        (IN_PORT, STOPPED) : [EVT_STOP],
        (STOPPED, AT_SEA)  : [EVT_START, EVT_EXIT],
        (STOPPED, IN_PORT) : [EVT_START],
        (STOPPED, STOPPED) : [],
        (None, AT_SEA)     : [],
        (None, IN_PORT)    : [],
        (None, STOPPED)    : [],
    }

    def __init__(self, anchorages, 
                 anchorage_entry_dist, anchorage_exit_dist,
                 stopped_begin_speed, stopped_end_speed,
                 min_gap_minutes, start_date, end_date):
        # TODO: use dates for start and end date
        self.anchorages = anchorages
        self.anchorage_entry_dist = anchorage_entry_dist
        self.anchorage_exit_dist = anchorage_exit_dist
        self.stopped_begin_speed = stopped_begin_speed
        self.stopped_end_speed = stopped_end_speed
        self.min_gap = timedelta(minutes=min_gap_minutes)
        assert self.min_gap < timedelta(days=1), 'min gap must be under one day in current implementation'
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
            return (state in (self.IN_PORT, self.STOPPED))

    def _is_stopped(self, state, speed):
        if speed <= self.stopped_begin_speed:
            return True
        elif speed >= self.stopped_end_speed:
            return False
        else:
            return (state == self.STOPPED) 

    def _anchorage_distance(self, loc, anchorages):
        closest = None
        min_dist = inf
        for anch in sorted(anchorages, key=lambda x: x.s2id):
            dist = distance(loc, anch.mean_location)
            if dist < min_dist:
                min_dist = dist
                closest = anch
        return closest, min_dist

    def _extract_records(self, items):
        n_records = len(items['records'])
        if n_records == 0:
            return []
        elif n_records == 1:
            return items['records'][0]
        else:
            raise ValueError('grouped_states_and_records should have 0 or 1 sets of records')

    def parse_datetime(self, text):
        # TODO: should check that %Z is what we think it is....
        naive = None
        try:
            naive = datetime.datetime.strptime(text, '%Y-%m-%d %H:%M:%S.%f %Z')
        except ValueError:
            naive = datetime.datetime.strptime(text, '%Y-%m-%d %H:%M:%S %Z')
        return naive.replace(tzinfo=pytz.utc)

    def parse_date(self, text):
        return datetime.datetime.strptime(text, '%Y-%m-%d').date()

    def date_as_datetime(self, date):
        return datetime.datetime.combine(date, datetime.time.min, tzinfo=pytz.utc)

    def _extract_state_info(self, items, anchorage_map):
        n_states = len(items['state'])
        if n_states == 0:
            return None, None, None
        elif n_states == 1:
            [state_info] = items['state']
            state = state_info['state']
            assert state in self.all_states, state
            if state_info['active_port'] is None:
                active_port = None
            else:
                s2id = state_info['active_port']
                active_port = anchorage_map.get(s2id)
                assert active_port is not None, (s2id, type(s2id),
                    list(anchorage_map.keys())[0], type(list(anchorage_map.keys())[0]))
                [active_port] = [x for x in active_port if x.s2id == s2id]
            date = self.parse_date(state_info['date'])
            last_timestamp = self.parse_datetime(state_info['last_timestamp'])
            assert date == self.start_date - timedelta(days=1)
            return last_timestamp, state, active_port
        else:
            raise ValueError('grouped_states_and_records should have 0 or 1 states')

    def _compute_state(self, is_in_port, is_stopped):
        if is_in_port:
            if is_stopped:
                return self.STOPPED
            else:
                return self.IN_PORT
        else:
            return self.AT_SEA


    def _build_event(self, active_port, rcd, seg_id, event_type, last_timestamp):
        return VisitEvent(anchorage_id=active_port.s2id, 
                          lat=active_port.mean_location.lat, 
                          lon=active_port.mean_location.lon, 
                          vessel_lat=rcd.location.lat,
                          vessel_lon=rcd.location.lon,
                          seg_id=seg_id, 
                          timestamp=rcd.timestamp, 
                          event_type=event_type,
                          last_timestamp=last_timestamp,
                         )

    def _yield_states(self, state_info_map):
        date = self.start_date
        state_info = state_info_map[date]
        while date <= self.end_date:
            state_info = state_info_map.get(date, state_info).copy()
            state_info['date'] = self.date_as_datetime(date)
            if state_info['state'] is not None:
                yield pvalue.TaggedOutput('state', state_info)
            date += timedelta(days=1)

    def _possibly_yield_gap_beg(self, seg_id, last_timestamp, last_state, next_timestamp, active_port):
        if next_timestamp - last_timestamp >= self.min_gap:
            if last_state in self.in_port_states:
                evt_timestamp = last_timestamp + self.min_gap
                if self.start_date <= evt_timestamp.date() <= self.end_date:
                    assert evt_timestamp <= next_timestamp
                    rcd = PseudoRcd(location=cmn.LatLon(None, None), timestamp=evt_timestamp)  
                    yield self._build_event(active_port, rcd, seg_id, self.EVT_GAP_BEG, last_timestamp)

    def _build_state(self, seg_id, date, state, active_port, last_timestamp):
        port_id = None if (active_port is None) else active_port.s2id
        return {'seg_id' : seg_id,
                'date' : date, 
                'state' : state, 
                'active_port' : port_id, 
                'last_timestamp' : last_timestamp}

    def create_in_out_events(self, grouped_states_and_records, anchorage_map):
        seg_id, items = grouped_states_and_records
        last_timestamp, last_state, active_port  = self._extract_state_info(items, anchorage_map)
        prev_state_info = self._build_state(seg_id, self.start_date, last_state, active_port, last_timestamp)
        state_info_map = {self.start_date : prev_state_info}

        rcd = None
        for rcd in self._extract_records(items):

            s2id = rcd.location.S2CellId(cmn.VISITS_S2_SCALE).to_token()
            port, dist = self._anchorage_distance(rcd.location, anchorage_map.get(s2id, []))
            is_in_port = self._is_in_port(last_state, dist)
            active_port = port if is_in_port else active_port
            is_stopped = self._is_stopped(last_state, rcd.speed)
            state = self._compute_state(is_in_port, is_stopped)

            if last_timestamp is not None:
                if (last_state in self.in_port_states and 
                    rcd.timestamp - last_timestamp >= self.min_gap and
                    last_timestamp.date() >= self.start_date - timedelta(days=1)):
                    # if is_in_port: # and last_state in (self.IN_PORT, self.STOPPED): # Current logic
                        yield self._build_event(active_port, rcd, seg_id, self.EVT_GAP_END, last_timestamp)
                yield from self._possibly_yield_gap_beg(seg_id, last_timestamp, last_state, rcd.timestamp, active_port)

            for event_type in self.transition_map[(last_state, state)]:
                yield self._build_event(active_port, rcd, seg_id, event_type, last_timestamp)

            date = rcd.timestamp.date()
            state_info_map[date] = self._build_state(seg_id, date, state, active_port, rcd.timestamp)

            last_timestamp = rcd.timestamp
            last_state = state

        end_time = datetime.datetime.combine(self.end_date, datetime.time.max, tzinfo=pytz.utc)
        yield from self._possibly_yield_gap_beg(seg_id, last_timestamp, last_state, end_time, active_port)
        yield from self._yield_states(state_info_map)



    def expand(self, grouped_states_and_records):
        anchorage_map = beam.pvalue.AsDict(self.anchorages)
        return (grouped_states_and_records
            | beam.FlatMap(self.create_in_out_events, anchorage_map=anchorage_map).with_outputs('state', main='records')
            )
