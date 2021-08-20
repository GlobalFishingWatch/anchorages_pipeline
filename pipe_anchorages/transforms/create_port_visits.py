from __future__ import absolute_import, print_function, division

import logging
import apache_beam as beam
import six
import hashlib
import math

from pipe_anchorages import common as cmn
from pipe_anchorages.objects.port_visit import PortVisit


class CreatePortVisits(beam.PTransform):

    EVENT_TYPES = ['PORT_ENTRY',
                   # The order of PORT_GAP_XXX is somewhat arbitrary, but it
                   # Shouldn't matter as long as it occurs between ENTRY
                   # and EXIT.
                   'PORT_GAP_BEGIN',
                   'PORT_GAP_END',
                   'PORT_STOP_BEGIN',
                   'PORT_STOP_END',
                   'PORT_EXIT']

    TYPE_ORDER = {x : i for (i, x) in enumerate(EVENT_TYPES)}

    MAX_EMITTED_EVENTS = 200

    def __init__(self, max_interseg_dist_nm):
        self.max_interseg_dist_nm = max_interseg_dist_nm

    def compute_confidence(self, events):
        event_types = set(x.event_type for x in events)
        has_stop = ('PORT_STOP_BEGIN' in event_types) or ('PORT_STOP_END' in event_types)
        has_gap = ('PORT_GAP_BEGIN' in event_types) or ('PORT_GAP_END' in event_types) 
        has_entry = 'PORT_ENTRY' in event_types
        has_exit = 'PORT_EXIT' in event_types
        if (has_stop or has_gap) and (has_entry and has_exit):
            return 4
        if (has_stop or has_gap) and (has_entry or has_exit):
            return 3
        if (has_stop or has_gap):
            return 2
        if (has_entry or has_exit):
            return 1
        raise ValueError(f'`events` missing expected event types. Has {set(event_types)}')

    def prune_events(self, events):
        if len(events) > self.MAX_EMITTED_EVENTS:
            n = self.MAX_EMITTED_EVENTS // 2
            events = events[:n] + events[-n:]
        return events

    def create_visit(self, id_, visit_events):
        ssvid, vessel_id = id_
        raw_visit_id = "{}-{}-{}-{}".format(vessel_id, 
                                visit_events[0].timestamp.isoformat(),
                                visit_events[0].lon, visit_events[0].lat)
        duration_hrs = ((visit_events[-1].timestamp - visit_events[0].timestamp)
                        .total_seconds() / (60 * 60))
        return PortVisit(visit_id=hashlib.md5(six.ensure_binary(raw_visit_id)).hexdigest(),
                         ssvid=str(ssvid),
                         vessel_id=str(vessel_id),
                         start_timestamp=visit_events[0].timestamp,
                         start_lat=visit_events[0].lat,
                         start_lon=visit_events[0].lon,
                         start_anchorage_id=visit_events[0].anchorage_id,
                         end_timestamp=visit_events[-1].timestamp,
                         end_lat=visit_events[-1].lat,
                         end_lon=visit_events[-1].lon,
                         end_anchorage_id=visit_events[-1].anchorage_id,
                         duration_hrs=duration_hrs,
                         confidence=self.compute_confidence(visit_events),
                         events=self.prune_events(visit_events))


    def possibly_yield_visit(self, id_, events):
        if events:
            yield self.create_visit(id_, events)

    def has_large_interseg_dist(self, evt1, evt2):
        if evt1.seg_id == evt2.seg_id:
            return False
        dlat = evt2.lat - evt1.lat
        lat = 0.5 * (evt1.lat + evt2.lat)
        scale = math.cos(math.radians(lat))
        dlon = evt2.lon - evt1.lon
        # Ensure dlon is in range [-180, 180] 
        # so that we don't have trouble near the dateline
        dlon = (x + 180) % 360 - 180
        dist_nm = math.hypot(dlat, scale * dlon) * 60 
        return dist_nm > self.max_interseg_dist_nm

    def create_port_visits(self, tagged_events):
        id_, events = tagged_events
        # Sort events by timestamp, and also so that enter, stop, start,
        # exit are in the correct order.
        tagged = [(x.timestamp, self.TYPE_ORDER[x.event_type], x)
                    for x in events]
        tagged.sort()
        ordered_events = [x for (_, _, x) in tagged]

        visit_events = []
        for i, evt in enumerate(ordered_events):
            has_large_gap = self.has_large_interseg_dist(visit_events[-1], evt) if visit_events else False
            if evt.event_type in 'PORT_ENTRY' or has_large_gap:
                yield from self.possibly_yield_visit(id_, visit_events)
                visit_events = []
            if evt.event_type not in self.EVENT_TYPES:
                logging.error(f'Unknown event type "{evt.event_type}", discarding.')
                continue
            visit_events.append(evt)
            is_last = (i == len(ordered_events) - 1)
            if (evt.event_type == 'PORT_EXIT') or is_last:
                yield from self.possibly_yield_visit(id_, visit_events)
                visit_events = []



    def expand(self, tagged_records):
        return (tagged_records
            | beam.GroupByKey()
            | beam.FlatMap(self.create_port_visits)
            )
