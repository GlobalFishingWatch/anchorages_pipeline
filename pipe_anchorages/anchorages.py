from __future__ import absolute_import, print_function, division

import argparse
import datetime
import s2sphere
import logging
import math
from collections import namedtuple, Counter
from .sparse_inland_mask import SparseInlandMask
from .distance import distance

from . import common as cmn
from .transforms.source import QuerySource
from .transforms.sink import AnchorageSink
from .port_name_filter import normalized_valid_names
from .find_anchorage_points import FindAnchoragePoints

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions



def parse_command_line_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--name', required=True, 
                        help='Name to prefix output and job name if not otherwise specified')
    # TODO: Replace
    parser.add_argument('--output', dest='output',
                        help='Output file to write results to.')
    parser.add_argument('--input-table', required=True,
                        help='Input table to pull data from')
    parser.add_argument('--start-date', required=True, 
                        help="First date to look for locations.")
    parser.add_argument('--end-date', required=True, 
                        help="Last date (inclusive) to look for locations.")
    parser.add_argument('--start-window', 
                        help="date to start tracking events to warm up vessel state")
    parser.add_argument('--config', default='anchorage_cfg.yaml',
                        help="path to configuration file")
    parser.add_argument('--fishing-mmsi-list',
                         dest='fishing_mmsi_list',
                         help='location of list of newline separated fishing mmsi')

    known_args, pipeline_args = parser.parse_known_args()

    if known_args.output is None:
        known_args.output = 'machine_learning_dev_ttl_30d.anchorages_{}'.format(known_args.name)

    cmn.add_pipeline_defaults(pipeline_args, known_args.name)

    pipeline_options = PipelineOptions(pipeline_args)
    cmn.check_that_pipeline_args_consumed(pipeline_options)

    return known_args, pipeline_options


def create_queries(args):
    template = """
    SELECT mmsi, lat, lon, timestamp, destination, speed FROM   
      TABLE_DATE_RANGE([world-fishing-827:{table}.], 
                        TIMESTAMP('{start:%Y-%m-%d}'), TIMESTAMP('{end:%Y-%m-%d}')) 
    """
    start_window = datetime.datetime.strptime(args.start_date, '%Y-%m-%d') 
    end_window = datetime.datetime.strptime(args.end_date, '%Y-%m-%d') 

    queries = []
    start = start_window
    while start < end_window:
        # Add 999 days so that we get 1000 total days
        end = min(start + datetime.timedelta(days=999), end_window)
        queries.append(template.format(table=args.input_table, start=start, end=end))
        # Add 1 day to end, so that we don't overlap.
        start = end + datetime.timedelta(days=1)

    return queries



def run():
    known_args, pipeline_options = parse_command_line_args()

    config = cmn.load_config(known_args.config)

    queries = create_queries(known_args)

    with open(known_args.fishing_mmsi_list) as f:
        fishing_vessel_set = set([int(x.strip()) for x in f.readlines() if x.strip()])

    inland_mask = SparseInlandMask()

    p = beam.Pipeline(options=pipeline_options)

    source = [(p | "Source_{}".format(i) >> QuerySource(query)) 
                for (i, query) in enumerate(queries)] | beam.Flatten()

    tagged_records = (source
        | cmn.CreateVesselRecords(config['blacklisted_mmsis'])
        | cmn.CreateTaggedRecords(config['min_required_positions'])
        )

    anchorage_points = (tagged_records
        | FindAnchoragePoints(datetime.timedelta(minutes=config['stationary_period_min_duration_minutes']), 
                              config['stationary_period_max_distance_km'],
                              config['min_unique_vessels_for_anchorage'],
                              fishing_vessel_set,
                              inland_mask)
        )

    (anchorage_points | AnchorageSink(table=known_args.output, 
                                      write_disposition="WRITE_TRUNCATE")
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

