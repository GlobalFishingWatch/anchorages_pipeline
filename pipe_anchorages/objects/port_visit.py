from collections import namedtuple
from .namedtuples import NamedtupleCoder

PortVisit = namedtuple("PortVisit", 
    ['visit_id', 'ssvid', 'vessel_id', 
     'start_timestamp', 'start_lat', 'start_lon', 'start_anchorage_id',
     'end_timestamp',   'end_lat',   'end_lon',   'end_anchorage_id', 
     'duration_hrs',
     'events', 'confidence'])


class PortVisitCoder(NamedtupleCoder):
    target = PortVisit
    time_fields = ['start_timestamp', 'end_timestamp']

PortVisitCoder.register()





