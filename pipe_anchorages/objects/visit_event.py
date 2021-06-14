from collections import namedtuple
from .namedtuples import NamedtupleCoder

VisitEvent = namedtuple("VisitEvent", 
    ['anchorage_id', 'lat', 'lon', 'vessel_lat', 'vessel_lon', 'seg_id', 'timestamp', 'event_type',
     'last_timestamp'])

class VisitEventCoder(NamedtupleCoder):
    target = VisitEvent
    time_fields = ['timestamp', 'last_timestamp']

VisitEventCoder.register()





