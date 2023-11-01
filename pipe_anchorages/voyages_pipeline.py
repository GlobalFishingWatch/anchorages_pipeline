from apache_beam.options.pipeline_options import (GoogleCloudOptions, StandardOptions)
from pipe_anchorages.options.voyages_options import VoyagesOptions
from apache_beam.runners import PipelineState

from pipe_anchorages.transforms.read_source import ReadSource

import apache_beam as beam
import logging
import datetime as dt
import pytz
import itertools


success_states = set([
    PipelineState.DONE,
    PipelineState.RUNNING,
    PipelineState.UNKNOWN,
    PipelineState.PENDING,
])

# Group by vessel_id and later order by exit of port visist.
class GroupByVessels(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | self.group_by_vessel()
            | self.sort_by_exit_port()
        )
    def group_by_vessel(self):
        return beam.GroupBy(lambda x: x['vessel_id'])
    def sort_by_exit_port(self):
        # Order by exit of the visit
        return beam.MapTuple(lambda k,v: (k, sorted(v, key=lambda x:x['end_timestamp'])))


# Writes the data
class WriteSink(beam.PTransform):
    def __init__(self, sink_table):
        self.sink_table = sink_table
    def expand(self, pcoll):
        return (
            pcoll
            | self.write_sink()
        )
    def write_sink(self):
        return beam.io.WriteToText('voyages')


DATE_DUMMY = dt.datetime(1,2,3).replace(tzinfo=pytz.UTC)
DURATION_START = dt.datetime(1970,1,1).replace(tzinfo=pytz.UTC)
DAYS_SECONDS = 24*60*60
duration = lambda d1: ((d:=d1-DURATION_START).days*DAYS_SECONDS + d.seconds)*1000 + d.microseconds//1000
trip_id_hex = lambda d1: format(d1, '012x')
c234 = lambda x: x['confidence'] >= 2
c34 = lambda x: x['confidence'] >= 3
c4 = lambda x: x['confidence'] == 4
# Creates the voyages
class CreateVoyages(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | self.create_voyages()
        )
    def create_voyages(self):
        return beam.MapTuple(lambda k, v: (k, self.get_voyages(v)))

    def get_voyages(self, visits):
        voyages_c2 = []
        voyages_c3 = []
        voyages_c4 = []
        visit_before_c2 = None
        visit_before_c3 = None
        visit_before_c4 = None

        for visit_before, visit_after in zip([None]+visits, visits+[None]):

            if visit_before == None:
                visit_before_c2 = visit_before_c3 = visit_before_c4 = visit_before
            else:
                if c234(visit_before):
                    visit_before_c2 = visit_before
                if c34(visit_before):
                    visit_before_c3 = visit_before
                if c4(visit_before):
                    visit_before_c4 = visit_before

            if visit_after == None:
                if visit_before_c2:
                    self.save_voyage(voyages_c2, visit_before_c2, visit_after)
                if visit_before_c3:
                    self.save_voyage(voyages_c3, visit_before_c3, visit_after)
                if visit_before_c4:
                    self.save_voyage(voyages_c4, visit_before_c4, visit_after)
            else:
                if c234(visit_after):
                    self.save_voyage(voyages_c2, visit_before_c2, visit_after)
                if c34(visit_after):
                    self.save_voyage(voyages_c3, visit_before_c3, visit_after)
                if c4(visit_after):
                    self.save_voyage(voyages_c4, visit_before_c4, visit_after)
        return (visits, [(2, voyages_c2),(3, voyages_c3),(4, voyages_c4)])

    def save_voyage(self, voyages, visit_before, visit_after):
            voyage = {}
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
            voyage['trip_id'] = f'{voyage["ssvid"]}-{voyage["vessel_id"]}' + f'-{trip_id_hex(duration(voyage["trip_start"]))}' if visit_before else ''
            # voyage['trip_confidence'] = ?
            voyages.append(voyage)



class VoyagesPipeline:
    def __init__(self, options):
        self.options = options
        params = options.view_as(VoyagesOptions)
        cloud_options = options.view_as(GoogleCloudOptions)

        self.pipeline = beam.Pipeline(options=options)
        (
            self.pipeline
            | ReadSource(params.source_table)
            | GroupByVessels()
            | CreateVoyages()
            | WriteSink(params.output_table)
        )

    def run(self):
        result = self.pipeline.run()
        if (
            options.view_as(VoyagesOptions).wait_for_job
            or options.view_as(StandardOptions).runner == "DirectRunner"
        ):
            result.wait_until_finish()
            # if result.state == PipelineState.DONE:
            #     sink.update_labels()

        logging.info("Returning with result.state=%s" % result.state)
        return 0 if result.state in success_states else 1

