from __future__ import absolute_import, print_function, division

import argparse
import datetime
from collections import namedtuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from . import common as cmn
from .distance import distance, inf
from .transforms.source import QuerySource
from .transforms.sink import EventSink


PortVisit = namedtuple("PortVisit", 
    ['anchorage_id', 'port_label', 'lat', 'lon', 'mmsi', 'start_time', 'end_time', 'events']) # Events is a list of portevents



class CreateInOutEvents(beam.PTransform):


    def __init__(self):
        pass


    def expand(self, xs):
        return xs



def parse_command_line_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--name', required=True, 
                        help='Name to prefix output and job name if not otherwise specified')
    parser.add_argument('--anchorages', 
                        help='Name of of anchorages table (BQ)')
    parser.add_argument('--output-table', dest='output',
                        help='Output table (BQ) to write results to.')
    parser.add_argument('--input-table', default='pipeline_classify_p_p429_resampling_2',
                        help='Input table to pull data from')
    parser.add_argument('--start-date', required=True, 
                        help="First date to look for entry/exit events.")
    parser.add_argument('--end-date', required=True, 
                        help="Last date (inclusive) to look for entry/exit events.")
    parser.add_argument('--config', default='config.yaml',
                        help="path to configuration file")
    parser.add_argument('--fast-test', action='store_true', 
                        help='limit query size for testing')

    known_args, pipeline_args = parser.parse_known_args()

    if known_args.output is None:
        known_args.output = 'machine_learning_dev_ttl_30d.in_out_events_{}'.format(known_args.name)

    cmn.add_pipeline_defaults(pipeline_args, known_args.name)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    cmn.check_that_pipeline_args_consumed(pipeline_options)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    return known_args, pipeline_options


def create_queries(args):
    template = """
    SELECT mmsi, lat, lon, timestamp, destination, speed FROM   
      TABLE_DATE_RANGE([world-fishing-827:{table}.], 
                        TIMESTAMP('{start:%Y-%m-%d}'), TIMESTAMP('{end:%Y-%m-%d}')) 
    """
    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d') 
    end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d') 
    while start_date <= end_date:
        start_window = start_date - datetime.timedelta(days=1)
        end_of_year = datetime.datetime(year=start_date.year, month=12, day=31)
        end_window = min(end_date, end_of_year)
        query = template.format(table=args.input_table, start=start_window, end=end_window)
        if args.fast_test:
            query += 'LIMIT 100000'
        yield query
        start_date = datetime.datetime(year=start_date.year + 1, month=1, day=1)



query = '''SELECT port_label, anchorage_id, mmsi, vessel_lon, vessel_lat, lat, lon, timestamp, event_type  
FROM [world-fishing-827:{}]'''


def run():
    known_args, pipeline_options = parse_command_line_args()

    p = beam.Pipeline(options=pipeline_options)

    config = cmn.load_config(known_args.config)

    (p
    | beam.io.Read(beam.io.gcp.bigquery.BigQuerySource(query=query.format(known_args.input_table)))
    | CreateEventsFromVisits()
    | VisitSink(table=known_args.output, write_disposition="WRITE_TRUNCATE")
    )


    result = p.run()
    result.wait_until_finish()

