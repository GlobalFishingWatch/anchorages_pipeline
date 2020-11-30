from collections import namedtuple
from .namedtuples import NamedtupleCoder

VisitEvent = namedtuple("VisitEvent", 
    ['anchorage_id', 'lat', 'lon', 'vessel_lat', 'vessel_lon', 'track_id', 'timestamp', 'event_type'])

class VisitEventCoder(NamedtupleCoder):
    target = VisitEvent
    time_fields = ['timestamp']

VisitEventCoder.register()





