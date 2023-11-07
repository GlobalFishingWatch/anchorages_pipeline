import apache_beam as beam
import datetime as dt
import pytz

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
        return beam.MapTuple(lambda k, v: list(self.create_voyage(v+[None])))

    def create_voyage(self, visits):
        # first voyage start from 'we do not know' = None
        visit_before = {2: None, 3: None, 4: None}

        def min_confidence(v, c:int)->bool:
            return v['confidence'] >= c

        for visit in visits:
            for c in [2,3,4]:
                if (visit and min_confidence(visit, c)) or (not visit and visit_before[c]):
                    yield self.build_voyage(c, visit_before[c], visit)
                    visit_before[c] = visit

    def build_voyage(self, trip_confidence, visit_before, visit_after):
        voyage = {}
        voyage['trip_confidence'] = trip_confidence
        voyage['ssvid'] = visit_before['ssvid'] if visit_before else visit_after['ssvid']
        voyage['vessel_id'] = visit_before['vessel_id'] if visit_before else visit_after['vessel_id']
        voyage['trip_start'] = visit_before['end_timestamp'] if visit_before else None
        voyage['trip_end'] = visit_after['start_timestamp'] if visit_after else None
        voyage['trip_start_anchorage_id'] = visit_before['end_anchorage_id'] if visit_before else None
        voyage['trip_end_anchorage_id'] = visit_after['start_anchorage_id'] if visit_after else None
        voyage['trip_start_visit_id'] = visit_before['visit_id'] if visit_before else None
        voyage['trip_end_visit_id'] = visit_after['visit_id'] if visit_after else None
        voyage['trip_start_confidence'] = visit_before['confidence'] if visit_before else None
        voyage['trip_end_confidence'] = visit_after['confidence'] if visit_after else None
        voyage['trip_id'] = f'{voyage["ssvid"]}-{voyage["vessel_id"]}' + (f'-{CreateVoyages.trip_id_hex(voyage["trip_start"])}' if visit_before else '')
        return voyage

    def expand(self, pcoll):
        return (
            pcoll
            | self.create_voyages()
            | beam.FlatMap(lambda x: x) # extracts from the list to one line elements
        )
