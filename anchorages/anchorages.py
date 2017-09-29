from __future__ import absolute_import, print_function, division

import argparse
import ujson as json
import datetime
import s2sphere
import math
from collections import namedtuple
from .sparse_inland_mask import SparseInlandMask
from .distance import distance

from . import common as cmn
from .transforms.source import QuerySource
from .transforms.sink import AnchorageSink

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

MAX_DESTINATIONS = 10

StationaryPeriod = namedtuple("StationaryPeriod", 
    ['location', 'start_time', 'duration', 'rms_drift_radius', 'destination'])

ActiveAndStationary = namedtuple("ActiveAndStationary", 
    ['active_records', 'stationary_periods'])


class FindAnchoragePoints(beam.PTransform):

    def __init__(self, min_duration, max_distance, min_unique_vessels, fishing_vessels):
        self.min_duration = min_duration
        self.max_distance = max_distance
        self.min_unique_vessels = min_unique_vessels
        self.fishing_vessels = fishing_vessels

    def split_on_movement(self, item):
        # extract long stationary periods from the record. Stationary periods are returned 
        # separately: anything over the threshold time will be reduced to just the start 
        # and end points of the period. The remaining points will summarized and returned
        # as a stationary period
        mmsi, records = item

        active_records = []
        stationary_periods = []
        current_period = []

        for rcd in records:
            if current_period:
                first_rcd = current_period[0]
                if distance(rcd.location, first_rcd.location) > self.max_distance:
                    if current_period[-1].timestamp - first_rcd.timestamp > self.min_duration:
                        active_records.append(first_rcd)
                        if current_period[-1] != first_rcd:
                            active_records.append(current_period[-1])
                        num_points = len(current_period)
                        duration = current_period[-1].timestamp - first_rcd.timestamp
                        mean_lat = sum(x.location.lat for x in current_period) / num_points
                        mean_lon = sum(x.location.lon for x in current_period) / num_points
                        mean_location = cmn.LatLon(mean_lat, mean_lon)
                        rms_drift_radius = math.sqrt(sum(distance(x.location, mean_location)**2 for x in current_period) / num_points)
                        stationary_periods.append(StationaryPeriod(mean_location, 
                                                                   first_rcd.timestamp,
                                                                   duration, 
                                                                   rms_drift_radius,
                                                                   first_rcd.destination))
                    else:
                        active_records.extend(current_period)
                    current_period = []
            current_period.append(rcd)
        active_records.extend(current_period)

        return (mmsi, ActiveAndStationary(active_records=active_records, 
                                          stationary_periods=stationary_periods))

    def extract_stationary(self, item):
        mmsi, combined = item
        return [(cmn.S2_cell_ID(sp.location).to_token(), (mmsi, sp)) 
                        for sp in combined.stationary_periods] 

    def extract_active(self, item):
        mmsi, combined = item
        return [(cmn.S2_cell_ID(ar.location).to_token(), (mmsi, ar)) 
                        for ar in combined.active_records] 


    def expand(self, ais_source):
        combined = ais_source | beam.Map(self.split_on_movement)
        stationary = combined | beam.FlatMap(self.extract_stationary)
        active =     combined | beam.FlatMap(self.extract_active)
        return ((stationary, active)
            | beam.CoGroupByKey()
            | beam.FlatMap(cmn.AnchoragePts_from_cell_visits, dest_limit=MAX_DESTINATIONS, 
                           fishing_vessel_set=self.fishing_vessels)
            | beam.Filter(lambda x: len(x.vessels) >= self.min_unique_vessels)
            )






def parse_command_line_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--name', required=True, 
                        help='Name to prefix output and job name if not otherwise specified')
    # TODO: Replace
    parser.add_argument('--input-patterns', default='all_years',
                        help='Input file to patterns (comma separated) to process (glob)')
    parser.add_argument('--output', dest='output',
                        help='Output file to write results to.')
    parser.add_argument('--input-table', default='pipeline_classify_p_p429_resampling_2',
                        help='Input table to pull data from')
    parser.add_argument('--start-date', required=True, 
                        help="First date to look for locations.")
    parser.add_argument('--end-date', required=True, 
                        help="Last date (inclusive) to look for locations.")
    parser.add_argument('--start-window', 
                        help="date to start tracking events to warm up vessel state")
    parser.add_argument('--config', default='config.yaml',
                        help="path to configuration file")
    # TODO: filter by latlon in this case
    # parser.add_argument('--fast-test', action='store_true', 
    #                     help='limit query size for testing')
    parser.add_argument('--fishing-mmsi-list',
                         dest='fishing_mmsi_list',
                         default='../treniformis/treniformis/_assets/GFW/FISHING_MMSI/KNOWN_LIKELY_AND_SUSPECTED/ANY_YEAR.txt',
                         help='location of list of newline separated fishing mmsi')

    known_args, pipeline_args = parser.parse_known_args()

    if known_args.output is None:
        known_args.output = 'machine_learning_dev_ttl_30d.in_out_events_{}'.format(known_args.name)

    # if known_args.output is None:
    #     known_args.output = 'gs://machine-learning-dev-ttl-30d/anchorages/{}/output'.format(known_args.name)

    cmn.add_pipeline_defaults(pipeline_args, known_args.name)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    cmn.check_that_pipeline_args_consumed(pipeline_options)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    return known_args, pipeline_options


def create_query(args):
    template = """
    SELECT mmsi, lat, lon, timestamp, destination, speed FROM   
      TABLE_DATE_RANGE([world-fishing-827:{table}.], 
                        TIMESTAMP('{start:%Y-%m-%d}'), TIMESTAMP('{end:%Y-%m-%d}')) 
    """
    start_window = datetime.datetime.strptime(args.start_date, '%Y-%m-%d') 
    end_window = datetime.datetime.strptime(args.end_date, '%Y-%m-%d') 

    query = template.format(table=args.input_table, start=start_window, end=end_window)

    return query



def run():
    known_args, pipeline_options = parse_command_line_args()

    config = cmn.load_config(known_args.config)

    query = create_query(known_args)

    with open(known_args.fishing_mmsi_list) as f:
        fishing_vessels = set([int(x.strip()) for x in f.readlines() if x.strip()])

    inland_mask = SparseInlandMask()

    p = beam.Pipeline(options=pipeline_options)

    tagged_records = (p 
        | "ReadAis" >> QuerySource(query)
        | cmn.CreateVesselRecords(config['blacklisted_mmsis'])
        | cmn.CreateTaggedRecords(config['min_required_positions'])
        )

    anchorage_points = (tagged_records
        | FindAnchoragePoints(datetime.timedelta(minutes=config['stationary_period_min_duration_minutes']), 
                              config['stationary_period_max_distance_km'],
                              config['min_unique_vessels_for_anchorage'],
                              fishing_vessels)
        )

    (anchorage_points 
        | "convertAPToJson" >> beam.Map(cmn.anchorage_point_to_json)
        | "writeAnchoragesPoints" >> AnchorageSink(table=known_args.output, 
                                                   write_disposition="WRITE_TRUNCATE",
                                                   max_destinations=MAX_DESTINATIONS)
    )

    result = p.run()
    result.wait_until_finish()

