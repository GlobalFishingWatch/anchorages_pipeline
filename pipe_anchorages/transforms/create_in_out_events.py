from __future__ import absolute_import, print_function, division

import apache_beam as beam

from pipe_anchorages import common as cmn
from pipe_anchorages.distance import distance, inf
from pipe_anchorages.objects.visit_event import VisitEvent


class CreateInOutEvents(beam.PTransform):

    IN_PORT = "IN_PORT"
    AT_SEA  = "AT_SEA"
    STOPPED = "STOPPED"

    EVT_ENTER = 'PORT_ENTRY'
    EVT_EXIT  = 'PORT_EXIT'
    EVT_STOP  = 'PORT_STOP_BEGIN'
    EVT_START = 'PORT_STOP_END'

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
                 stopped_begin_speed, stopped_end_speed):
        self.anchorages = anchorages
        self.anchorage_entry_dist = anchorage_entry_dist
        self.anchorage_exit_dist = anchorage_exit_dist
        self.stopped_begin_speed = stopped_begin_speed
        self.stopped_end_speed = stopped_end_speed

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
        for anch in anchorages:
            dist = distance(loc, anch.mean_location)
            if dist < min_dist:
                min_dist = dist
                closest = anch
        return closest, min_dist

    def create_in_out_events(self, tagged_records, anchorage_map):
        mmsi, records = tagged_records
        state = None
        active_port = None
        events = []
        for rcd in records:
            s2id = rcd.location.S2CellId(cmn.VISITS_S2_SCALE).to_token()
            port, dist = self._anchorage_distance(rcd.location, anchorage_map.get(s2id, []))

            last_state = state
            is_in_port = self._is_in_port(state, dist)
            is_stopped = self._is_stopped(state, rcd.speed)

            if is_in_port:
                active_port = port
                state = self.STOPPED if is_stopped else self.IN_PORT
            else:
                state = self.AT_SEA

            if (last_state is None) or (active_port is None):
                # Not enough information yet.
                continue 

            for event_type in self.transition_map[(last_state, state)]:
                events.append(VisitEvent(anchorage_id=active_port.s2id, 
                                         lat=active_port.mean_location.lat, 
                                         lon=active_port.mean_location.lon, 
                                         vessel_lat=rcd.location.lat,
                                         vessel_lon=rcd.location.lon,
                                         mmsi=mmsi, 
                                         timestamp=rcd.timestamp, 
                                         port_label=active_port.port_name, 
                                         event_type=event_type)) 
        return events

    def expand(self, tagged_records):
        anchorage_map = beam.pvalue.AsSingleton(self.anchorages)
        return (tagged_records
            | beam.FlatMap(self.create_in_out_events, anchorage_map=anchorage_map)
            )
