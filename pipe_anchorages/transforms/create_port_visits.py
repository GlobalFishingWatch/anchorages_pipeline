from __future__ import absolute_import, print_function, division

import logging
import apache_beam as beam

from pipe_anchorages import common as cmn
from pipe_anchorages.objects.port_visit import PortVisit


class CreatePortVisits(beam.PTransform):

    IN_PORT = "IN_PORT"
    AT_SEA  = "AT_SEA"

    TYPE_ORDER = {x : i for (i, x) in 
        enumerate(['PORT_ENTRY',
                   'PORT_STOP_BEGIN',
                   'PORT_STOP_END',
                   'PORT_EXIT'])}

    def __init__(self):
        pass

    def _is_in_port(self, state, dist):
        if dist <= self.anchorage_entry_dist:
            return True
        elif dist >= self.anchorage_exit_dist:
            return False
        else:
            return (state in (self.IN_PORT, self.STOPPED))

    def create_visit(self, visit_events):
        return PortVisit(vessel_id=str(visit_events[0].mmsi),
                         start_timestamp=visit_events[0].timestamp,
                         start_lat=visit_events[0].lat,
                         start_lon=visit_events[0].lon,
                         start_anchorage_id=visit_events[0].anchorage_id,
                         end_timestamp=visit_events[-1].timestamp,
                         end_lat=visit_events[-1].lat,
                         end_lon=visit_events[-1].lon,
                         end_anchorage_id=visit_events[-1].anchorage_id,
                         events=visit_events)

    def create_port_visits(self, tagged_events):
        mmsi, events = tagged_events
        # Sort events by timestamp, and also so that enter, stop, start,
        # exit are in the correct order.
        tagged = [(x.timestamp, self.TYPE_ORDER[x.event_type], x)
                    for x in events]
        tagged.sort()
        events = [x for (_, _, x) in tagged]

        visit_events = None
        for evt in events:
            if evt.event_type == 'PORT_ENTRY':
                if visit_events is not None:
                    logging.warning('PORT_ENTRY without earlier exit.\n'
                                    'Disarding previous event')
                visit_events = [evt]
                continue
            if visit_events is None:
                logging.warning('non PORT_ENTRY without earlier entry.\n'
                                'Disarding')
                continue

            if evt.event_type not in ('PORT_STOP_BEGIN', 
                                      'PORT_STOP_END',
                                      'PORT_EXIT'):
                logging.error('Unknown event type: %s\n'
                              'Discarding', evt.event_type)
                continue

            visit_events.append(evt)

            if evt.event_type == 'PORT_EXIT':
                yield self.create_visit(visit_events)
                visit_events = None



    def expand(self, tagged_records):
        return (tagged_records
            | beam.Map(lambda x: (x.mmsi, x))
            | beam.GroupByKey()
            | beam.FlatMap(self.create_port_visits)
            )