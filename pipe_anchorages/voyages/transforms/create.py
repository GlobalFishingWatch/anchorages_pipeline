import apache_beam as beam
import datetime as dt
import pytz

def create_voyage(visits):
    # first voyage start from 'we do not know' = None
    previous_visit = {2: None, 3: None, 4: None}

    def has_duration(v1, v2):
        return v1 == None or v1['end_timestamp'] < v2['start_timestamp']

    for visit in visits:
        for c in [2,3,4]:
            if (visit and visit['confidence'] >= c and has_duration(previous_visit[c],visit)) or (not visit and previous_visit[c]):
                yield build_voyage(c, previous_visit[c], visit)
                previous_visit[c] = visit

def build_voyage(trip_confidence, start_visit, end_visit):
    def maybe_get(key, visit, default=None):
        return visit[key] if visit else default

    def get_from_either(key, first_dict, second_dict):
        return first_dict[key] if first_dict else second_dict[key]

    voyage_ssvid = get_from_either('ssvid', start_visit, end_visit)
    voyage_vessel_id = get_from_either('vessel_id', start_visit, end_visit)
    voyage_trip_start = maybe_get('end_timestamp',  start_visit)
    voyage = {
        'trip_confidence': trip_confidence,
        'ssvid': voyage_ssvid,
        'vessel_id': voyage_vessel_id,
        'trip_start': voyage_trip_start,
        'trip_end': maybe_get('start_timestamp', end_visit),
        'trip_start_anchorage_id': maybe_get('end_anchorage_id', start_visit),
        'trip_end_anchorage_id': maybe_get('start_anchorage_id', end_visit),
        'trip_start_visit_id': maybe_get('visit_id', start_visit),
        'trip_end_visit_id': maybe_get('visit_id', end_visit),
        'trip_start_confidence': maybe_get('confidence', start_visit),
        'trip_end_confidence': maybe_get('confidence', end_visit),
        'trip_id': f'{voyage_ssvid}-{voyage_vessel_id}' + (f'-{CreateVoyages.trip_id_hex(voyage_trip_start)}' if start_visit else ''),
    }
    return voyage

"""
Creates the voyages
"""
class CreateVoyages(beam.PTransform):

    DURATION_START = dt.datetime(1970,1,1).replace(tzinfo=pytz.UTC)
    DAYS_SECONDS = 24*60*60

    def trip_id_hex(d1:dt.datetime)->str:
        """ Returns an hex with 12 padded zero of a duration from 1970-01-01 to d1. """
        duration = lambda d1: ((d:=d1-CreateVoyages.DURATION_START).days*CreateVoyages.DAYS_SECONDS + d.seconds)*1000 + d.microseconds//1000
        return format(duration(d1), '012x')

    def create_voyages(self):
        return beam.FlatMapTuple(lambda k, v: list(create_voyage(v+[None])))


    def expand(self, pcoll):
        return (
            pcoll
            | self.create_voyages()
        )
