from __future__ import absolute_import, print_function, division

import logging
import apache_beam as beam
import six
import hashlib

from pipe_anchorages import common as cmn
from pipe_anchorages.objects.port_visit import PortVisit


class CreatePortVisits(beam.PTransform):

    TYPE_ORDER = {x : i for (i, x) in 
        enumerate(['PORT_ENTRY',
                   # The order of PORT_GAP is somewhat arbitrary, but it
                   # Shouldn't matter as long as it occurs between ENTRY
                   # and EXIT.
                   'PORT_GAP',
                   'PORT_STOP_BEGIN',
                   'PORT_STOP_END',
                   'PORT_EXIT'])}

    def __init__(self):
        pass

    def create_visit(self, visit_events):
        raw_visit_id = "{}-{}-{}".format(vessel_id, 
                                visit_events[0].timestamp.isoformat(),
                                visit_events[-1].timestamp.isoformat())
        return PortVisit(visit_id=haslib.md5(six.ensure_binary(raw_visit_id).hexdigest()),
                         vessel_id=str(visit_events[0].vessel_id),
                         start_timestamp=visit_events[0].timestamp,
                         start_lat=visit_events[0].lat,
                         start_lon=visit_events[0].lat,
                         start_anchorage_id=visit_events[0].anchorage_id,
                         end_timestamp=visit_events[-1].timestamp,
                         end_lat=visit_events[-1].lat,
                         end_lon=visit_events[-1].lon,
                         end_anchorage_id=visit_events[-1].anchorage_id,
                         events=visit_events)

    def create_port_visits(self, tagged_events):
        vessel_id, events = tagged_events
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
                is_visit = False
                continue
            if visit_events is None:
                logging.warning('non PORT_ENTRY without earlier entry.\n'
                                'Disarding')
                continue

            if evt.event_type not in ('PORT_STOP_BEGIN', 
                                      'PORT_STOP_END',
                                      'PORT_EXIT',
                                      'PORT_GAP'):
                logging.error('Unknown event type: %s\n'
                              'Discarding', evt.event_type)
                continue

            visit_events.append(evt)

            if evt.event_type in ('PORT_STOP_BEGIN', 'PORT_GAP'):
                is_visit = True

            if evt.event_type == 'PORT_EXIT':
                # Only yield a visit if this qualifies as a visit; that is
                # there has been a stop or a gap.
                if is_visit == True:
                    yield self.create_visit(visit_events)
                visit_events = None



    def expand(self, tagged_records):
        return (tagged_records
            | beam.Map(lambda x: (x.vessel_id, x))
            | beam.GroupByKey()
            | beam.FlatMap(self.create_port_visits)
            )