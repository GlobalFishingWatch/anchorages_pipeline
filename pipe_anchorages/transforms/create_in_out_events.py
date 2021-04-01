from __future__ import absolute_import, print_function, division

import datetime
from datetime import timedelta
import pytz
import apache_beam as beam
from apache_beam import pvalue

from pipe_anchorages import common as cmn
from pipe_anchorages.distance import distance, inf
from pipe_anchorages.objects.visit_event import VisitEvent
from pipe_anchorages.objects.namedtuples import s_to_datetime
import logging


class CreateInOutEvents(beam.PTransform):

    IN_PORT = "IN_PORT"
    AT_SEA  = "AT_SEA"
    STOPPED = "STOPPED"

    EVT_ENTER = 'PORT_ENTRY'
    EVT_EXIT  = 'PORT_EXIT'
    EVT_STOP  = 'PORT_STOP_BEGIN'
    EVT_START = 'PORT_STOP_END'
    EVT_GAP   = 'PORT_GAP'

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
    }

    def __init__(self, anchorages, 
                 anchorage_entry_dist, anchorage_exit_dist,
                 stopped_begin_speed, stopped_end_speed,
                 min_gap_minutes, start_date, end_date):
        self.anchorages = anchorages
        self.anchorage_entry_dist = anchorage_entry_dist
        self.anchorage_exit_dist = anchorage_exit_dist
        self.stopped_begin_speed = stopped_begin_speed
        self.stopped_end_speed = stopped_end_speed
        self.min_gap_minutes = min_gap_minutes
        self.start_date = start_date
        self.end_date = end_date
        assert isinstance(self.start_date, datetime.datetime)

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
        naive = datetime.datetime.strptime(state_info['date'], '%Y-%m-%d %H:%M:%S.%f %Z')
        return naive.replace(tzinfo=pytz.utc)


    def _extract_state_info(self, items, anchorage_map):
        n_states = len(items['state'])
        if n_states == 0:
            return None, None, None
        elif n_states == 1:
            [state_info] = items['state']
            state = state_info['state']
            assert state in ("IN_PORT", "AT_SEA", "STOPPED"), state
            if state_info['active_port'] is None:
                active_port = None
            else:
                s2id = state_info['active_port']
                active_port = anchorage_map.get(s2id)
                assert active_port is not None, (s2id, type(s2id),
                    list(anchorage_map.keys())[0], type(list(anchorage_map.keys())[0]))
                [active_port] = [x for x in active_port if x.s2id == s2id]
            date = self.parse_datetime(state_info['date'])
            last_timestamp = self.parse_datetime(state_info['last_timestamp'])
            assert date == self.start_date - timedelta(days=1)
            return state, active_port, last_timestamp
        else:
            raise ValueError('grouped_states_and_records should have 0 or 1 states')

    def create_in_out_events(self, grouped_states_and_records, anchorage_map):
        seg_id, items = grouped_states_and_records
        state, active_port, last_timestamp = self._extract_state_info(items, anchorage_map)
        port_id = None if (active_port is None) else active_port.s2id
        state_info_map = {self.start_date : {
                    'seg_id' : seg_id,
                    'date' : self.start_date, 
                    'state' : state, 
                    'active_port' : port_id, 
                    'last_timestamp' : last_timestamp
                }}

        for rcd in self._extract_records(items):
            last_state = state

            s2id = rcd.location.S2CellId(cmn.VISITS_S2_SCALE).to_token()
            assert isinstance(s2id, str), (type(s2id), s2id)
            port, dist = self._anchorage_distance(rcd.location, anchorage_map.get(s2id, []))

            is_in_port = self._is_in_port(state, dist)
            is_stopped = self._is_stopped(state, rcd.speed)

            event_types = []
            if is_in_port:
                active_port = port
                state = self.STOPPED if is_stopped else self.IN_PORT
                if last_timestamp is not None:
                    delta_minutes = (rcd.timestamp - last_timestamp).total_seconds() / 60.0
                    if last_state in ('IN_PORT', 'STOPPED') and delta_minutes >= self.min_gap_minutes:
                        event_types.append(self.EVT_GAP)
            else:
                state = self.AT_SEA


            date = datetime.datetime.combine(rcd.timestamp.date(), datetime.time(), tzinfo=pytz.utc)
            active_port_id = None if (active_port is None) else active_port.s2id
            state_info_map[date] = {
                    'seg_id' : seg_id,
                    'date' : date, 
                    'state' : state, 
                    'active_port' : active_port_id, 
                    'last_timestamp' : rcd.timestamp
                }


            if (last_state is not None) and (active_port is not None):
                event_types.extend(self.transition_map[(last_state, state)])

                for etype in event_types:
                    yield VisitEvent(anchorage_id=active_port.s2id, 
                                     lat=active_port.mean_location.lat, 
                                     lon=active_port.mean_location.lon, 
                                     vessel_lat=rcd.location.lat,
                                     vessel_lon=rcd.location.lon,
                                     seg_id=seg_id, 
                                     timestamp=rcd.timestamp, 
                                     event_type=etype,
                                     last_timestamp=last_timestamp,
                                     )

            last_timestamp = rcd.timestamp


        date = self.start_date
        state_info = state_info_map[date]
        while date <= self.end_date:
            state_info = state_info_map.get(date, state_info).copy()
            state_info['date'] = date
            if state_info['state'] is not None:
                yield pvalue.TaggedOutput('state', state_info)
            date += timedelta(days=1)


    def expand(self, grouped_states_and_records):
        anchorage_map = beam.pvalue.AsDict(self.anchorages)
        return (grouped_states_and_records
            | beam.FlatMap(self.create_in_out_events, anchorage_map=anchorage_map).with_outputs('state', main='records')
            )
