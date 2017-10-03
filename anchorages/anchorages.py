from __future__ import absolute_import, print_function, division

import argparse
import datetime
import s2sphere
import math
from collections import namedtuple, Counter
from .sparse_inland_mask import SparseInlandMask
from .distance import distance

from . import common as cmn
from .transforms.source import QuerySource
from .transforms.sink import AnchorageSink
from .port_name_filter import normalized_valid_names
from .nearest_port import wpi_finder
from .nearest_port import geo_finder

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

StationaryPeriod = namedtuple("StationaryPeriod", 
    ['location', 'start_time', 'duration', 'rms_drift_radius', 'destination'])

ActiveAndStationary = namedtuple("ActiveAndStationary", 
    ['active_records', 'stationary_periods'])


class FindAnchoragePoints(beam.PTransform):

    def __init__(self, min_duration, max_distance, min_unique_vessels, fishing_vessel_set):
        self.min_duration = min_duration
        self.max_distance = max_distance
        self.min_unique_vessels = min_unique_vessels
        self.fishing_vessel_set = fishing_vessel_set

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
        return [(sp.location.S2CellId(cmn.ANCHORAGES_S2_SCALE).to_token(), (mmsi, sp)) 
                        for sp in combined.stationary_periods] 

    def extract_active(self, item):
        mmsi, combined = item
        return [(ar.location.S2CellId(cmn.ANCHORAGES_S2_SCALE).to_token(), (mmsi, ar)) 
                        for ar in combined.active_records] 

    def create_anchorage_pts(self, item):
        value = AnchoragePoint.from_cell_visits(item, self.fishing_vessel_set)
        return [] if (value is None) else [value]

    def has_enough_vessels(self, item):
        return len(item.vessels) >= self.min_unique_vessels

    def expand(self, ais_source):
        combined = ais_source | beam.Map(self.split_on_movement)
        stationary = combined | beam.FlatMap(self.extract_stationary)
        active =     combined | beam.FlatMap(self.extract_active)
        return ((stationary, active)
            | beam.CoGroupByKey()
            | beam.FlatMap(self.create_anchorage_pts)
            | beam.Filter(self.has_enough_vessels)
            )


class AnchoragePoint(namedtuple("AnchoragePoint", ['mean_location',
                                                  'total_visits',
                                                  'vessels',
                                                  'fishing_vessels',
                                                  'rms_drift_radius',
                                                  'top_destination',
                                                  's2id',
                                                  'neighbor_s2ids',
                                                  'active_mmsi',
                                                  'total_mmsi',
                                                  'stationary_mmsi_days',
                                                  'stationary_fishing_mmsi_days',
                                                  'active_mmsi_days',
                                                  'wpi_name',
                                                  'wpi_distance',
                                                  'geonames_name',
                                                  'geonames_distance'
                                               ])):
    __slots__ = ()


    @staticmethod
    def from_cell_visits(value, fishing_vessel_set):
        s2id, (stationary_periods, active_points) = value

        n = 0
        total_lat = 0.0
        total_lon = 0.0
        fishing_vessels = set()
        vessels = set()
        total_squared_drift_radius = 0.0
        active_mmsi = set(md for (md, loc) in active_points)
        active_mmsi_count = len(active_mmsi)
        active_days = len(set([(md, loc.timestamp.date()) for (md, loc) in active_points]))
        stationary_days = 0
        stationary_fishing_days = 0

        for (mmsi, sp) in stationary_periods:
            n += 1
            total_lat += sp.location.lat
            total_lon += sp.location.lon
            vessels.add(mmsi)
            stationary_days += sp.duration.total_seconds() / (24.0 * 60.0 * 60.0)
            if mmsi in fishing_vessel_set:
                fishing_vessels.add(mmsi)
                stationary_fishing_days += sp.duration.total_seconds() / (24.0 * 60.0 * 60.0)
            total_squared_drift_radius += sp.rms_drift_radius ** 2
        all_destinations = normalized_valid_names(sp.destination for (md, sp) in stationary_periods)

        total_mmsi_count = len(vessels | active_mmsi)

        if n:
            neighbor_s2ids = tuple(s2sphere.CellId.from_token(s2id).get_all_neighbors(cmn.ANCHORAGES_S2_SCALE))
            loc = cmn.LatLon(total_lat / n, total_lon / n)
            wpi_name, wpi_distance = wpi_finder.find_nearest_port_and_distance(loc)
            geo_name, geo_distance = geo_finder.find_nearest_port_and_distance(loc)

            all_destinations = list(all_destinations)
            if len(all_destinations):
                [(top_destination, top_count)] = Counter(all_destinations).most_common(1)
            else:
                top_destination = ''

            return AnchoragePoint(
                        mean_location = loc,
                        total_visits = n, 
                        vessels = frozenset(vessels),
                        fishing_vessels = frozenset(fishing_vessels),
                        rms_drift_radius =  math.sqrt(total_squared_drift_radius / n),    
                        top_destination = top_destination,
                        s2id = s2id,
                        neighbor_s2ids = neighbor_s2ids,
                        active_mmsi = active_mmsi_count,
                        total_mmsi = total_mmsi_count,
                        stationary_mmsi_days = stationary_days,
                        stationary_fishing_mmsi_days = stationary_fishing_days,
                        active_mmsi_days = active_days,
                        wpi_name = wpi_name,
                        wpi_distance = wpi_distance,
                        geonames_name = geo_name,
                        geonames_distance = geo_distance
                        )
        else:
            return None


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
    parser.add_argument('--fishing-mmsi-list',
                         dest='fishing_mmsi_list',
                         default='../treniformis/treniformis/_assets/GFW/FISHING_MMSI/KNOWN_LIKELY_AND_SUSPECTED/ANY_YEAR.txt',
                         help='location of list of newline separated fishing mmsi')

    known_args, pipeline_args = parser.parse_known_args()

    if known_args.output is None:
        known_args.output = 'machine_learning_dev_ttl_30d.anchorages_{}'.format(known_args.name)

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
                              fishing_vessel_set)
        )

    (anchorage_points | AnchorageSink(table=known_args.output, 
                                      write_disposition="WRITE_TRUNCATE")
    )

    result = p.run()
    result.wait_until_finish()

