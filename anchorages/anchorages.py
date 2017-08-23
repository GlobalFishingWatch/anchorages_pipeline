from __future__ import absolute_import, print_function, division

import argparse
import logging
import re
import ujson as json
import datetime
from collections import namedtuple, Counter, defaultdict
import itertools as it
import os
import math
import s2sphere
from .port_name_filter import normalized_valid_names
from .union_find import UnionFind
from .sparse_inland_mask import SparseInlandMask

# TODO put unit reg in package if we refactor
# import pint
# unit = pint.UnitRegistry()

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions




inland_mask = SparseInlandMask()



VesselMetadata = namedtuple('VesselMetadata', ['mmsi'])

def VesselMetadata_from_msg(msg):
    return VesselMetadata(msg['mmsi'])


TaggedVesselLocationRecord = namedtuple('TaggedVesselLocationRecord',
            ['destination', 's2id', 'is_new_id', 'timestamp', 'location', 'distance_from_shore', 'speed', 'course'])

VesselInfoRecord = namedtuple('VesselInfoRecord',
            ['timestamp', 'destination'])


AnchorageVisit = namedtuple('AnchorageVisit',
            ['anchorage', 'arrival', 'departure'])


class VesselLocationRecord(
    namedtuple("VesselLocationRecord",
              ['timestamp', 'location', 'distance_from_shore', 'speed', 'course'])):

    @classmethod
    def from_msg(cls, msg):
        latlon = LatLon(msg['lat'], msg['lon'])

        return cls(
            datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ'), 
            latlon, 
            msg['distance_from_shore'] / 1000.0,
            round(msg['speed'], 1),
            msg['course']            )



def Records_from_msg(msg, blacklisted_mmsis):

    mmsi = msg.get('mmsi')
    if not isinstance(mmsi, int) or (mmsi in blacklisted_mmsis):
        return []

    metadata = VesselMetadata_from_msg(msg)

    if is_location_message(msg):
        return [(metadata, VesselLocationRecord.from_msg(msg))]
    elif msg.get('destination') not in set(['', None]):
        return [(metadata, VesselInfoRecord(
            datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ'),
            msg['destination']
            ))]
    else:
        return []


AnchoragePoint = namedtuple("AnchoragePoint", ['mean_location',
                                               'total_visits',
                                               'vessels',
                                               'mean_distance_from_shore',
                                               'rms_drift_radius',
                                               'top_destinations',
                                               's2id',
                                               'neighbor_s2ids',
                                               'active_mmsi',
                                               'total_mmsi',
                                               'stationary_mmsi_days',
                                               'active_mmsi_days'
                                               ])



def is_location_message(msg):
    return (
                'lat' in msg and 
                'lon' in msg and
                'speed' in msg and
                'distance_from_shore' in msg and
                'course' in msg
        )


def is_not_bad_value(lr):
    return isinstance(lr, VesselInfoRecord) or (
    -90 <= lr.location.lat <= 90 and
    -180 <= lr.location.lon <= 180 and
    0 <= lr.distance_from_shore <= 20000 and
    0 <= lr.course < 360 
    )


def read_json_records(input, blacklisted_mmsis, latlon_filters):
    parsed =  (input 
        | "Parse" >> beam.Map(json.loads)
        )

    if latlon_filters is not None:
        parsed = (parsed 
            | "filterByLatLon" >> beam.Filter(filter_by_latlon, latlon_filters)
        )

    return (parsed 
        | "CreateLocationRecords" >> beam.FlatMap(Records_from_msg, blacklisted_mmsis)
        | "FilterOutBadValues" >> beam.Filter(lambda (md, lr): is_not_bad_value(lr))
        )

def tag_with_destination_and_id(records):
    """filter out info messages and use them to tag subsequent records"""
    # TODO: think about is_new_id, we don't currently use it. Original thought was to count
    # the average number of vessels in a cell at one time. For that we probably want to swithc
    # to is_first, and is_last for the first and last points in the cell. Could be done by
    # modifying the last item in the cell as well.
    dest = ''
    new = []
    last_s2id = None
    for rcd in records:
        if isinstance(rcd, VesselInfoRecord):
            # TODO: normalize here rather than later. And cache normalization in dictionary
            dest = rcd.destination
        else:
            s2id = S2_cell_ID(rcd.location).to_token()
            is_new_id = (s2id == last_s2id)
            new.append(TaggedVesselLocationRecord(dest, s2id, is_new_id,
                                                  *rcd)) 
            last_s2id = is_new_id
    return new

def dedup_by_timestamp(element):
    key, source = element
    seen = set()
    sink = []
    for x in source:
        if x.timestamp not in seen:
            sink.append(x)
            seen.add(x.timestamp)
    return (key, sink)



def filter_duplicate_timestamps(input, min_required_positions):
    return (input
           | "RemoveDupTStamps" >> beam.Map(dedup_by_timestamp)
           | "RemoveShortSeries" >> beam.Filter(lambda x: len(x[1]) >= min_required_positions)
           )
 

FIVE_MINUTES = datetime.timedelta(minutes=5)

def thin_points(records):
    # TODO: consider instead putting on five minute intervals? 
    if not records:
        return
    last = records[0]
    yield last
    for vlr in records[1:]:
        if (vlr.timestamp - last.timestamp) >= FIVE_MINUTES:
            last = vlr
            yield vlr


EARTH_RADIUS = 6371 # kilometers

def distance(a, b):
    h = ( math.sin(math.radians(a.lat - b.lat) / 2) ** 2 
        + math.cos(math.radians(a.lat)) * math.cos(math.radians(b.lat)) * math.sin(math.radians((a.lon - b.lon)) / 2) ** 2)
    h = min(h, 1)
    return 2 * EARTH_RADIUS * math.asin(math.sqrt(h))

StationaryPeriod = namedtuple("StationaryPeriod", ['location', 'duration', 'mean_distance_from_shore', 
        'rms_drift_radius', 'destination', 's2id'])

LatLon = namedtuple("LatLon", ["lat", "lon"])

def LatLon_from_LatLng(latlng):
    return LatLon(latlng.lat(), latlng.lon())

ProcessedLocations = namedtuple("ProcessedLocations", ['locations', 'stationary_periods'])


def remove_stationary_periods(records, stationary_period_min_duration, stationary_period_max_distance):
    # Remove long stationary periods from the record: anything over the threshold
    # time will be reduced to just the start and end points of the period.

    without_stationary_periods = []
    stationary_periods = []
    current_period = []

    for vr in records:
        if current_period:
            first_vr = current_period[0]
            if distance(vr.location, first_vr.location) > stationary_period_max_distance:
                if current_period[-1].timestamp - first_vr.timestamp > stationary_period_min_duration:
                    without_stationary_periods.append(first_vr)
                    if current_period[-1] != first_vr:
                        without_stationary_periods.append(current_period[-1])
                    num_points = len(current_period)
                    duration = current_period[-1].timestamp - first_vr.timestamp
                    mean_lat = sum(x.location.lat for x in current_period) / num_points
                    mean_lon = sum(x.location.lon for x in current_period) / num_points
                    mean_location = LatLon(mean_lat, mean_lon)
                    mean_distance_from_shore = sum(x.distance_from_shore for x in current_period) / num_points
                    rms_drift_radius = math.sqrt(sum(distance(x.location, mean_location)**2 for x in current_period) / num_points)
                    stationary_periods.append(StationaryPeriod(mean_location, duration, 
                                                               mean_distance_from_shore, rms_drift_radius,
                                                               first_vr.destination,
                                                               s2id=S2_cell_ID(mean_location).to_token()))
                else:
                    without_stationary_periods.extend(current_period)
                current_period = []
        current_period.append(vr)
    without_stationary_periods.extend(current_period)

    return ProcessedLocations(without_stationary_periods, stationary_periods) 


def filter_and_process_vessel_records(input, stationary_period_min_duration, stationary_period_max_distance):
    return ( input 
            | "splitIntoStationaryNonstationaryPeriods" >> beam.Map( lambda (metadata, records):
                        (metadata, 
                         remove_stationary_periods(records, stationary_period_min_duration, stationary_period_max_distance)))

            # | "ExtractStationaryPeriods" >> beam.Map(lambda (md, x): (md, x.stationary_periods))
            )

# Around 1km^2
ANCHORAGES_S2_SCALE = 13

def S2_cell_ID(loc):
    ll = s2sphere.LatLng.from_degrees(loc.lat, loc.lon)
    return s2sphere.CellId.from_lat_lng(ll).parent(ANCHORAGES_S2_SCALE)

def mean(iterable):
    n = 0
    total = 0.0
    for x in iterable:
        total += x
        n += 1
    return (total / n) if n else 0

def LatLon_mean(seq):
    seq = list(seq)
    return LatLon(mean(x.lat for x in seq), mean(x.lon for x in seq))


#TODO: use setup.py to break into multiple files (https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/)


bogus_destinations = set([''])

def AnchoragePts_from_cell_visits2(value, dest_limit):
    s2id, (stationary_periods, active_points) = value

    n = 0
    total_lat = 0.0
    total_lon = 0.0
    vessels = set()
    total_distance_from_shore = 0.0
    total_squared_drift_radius = 0.0
    active_mmsi = set(md for (md, loc) in active_points)
    active_mmsi_count = len(active_mmsi)
    active_days = len(set([(md, loc.timestamp.date()) for (md, loc) in active_points]))
    stationary_days = 0

    for (md, sp) in stationary_periods:
        n += 1
        total_lat += sp.location.lat
        total_lon += sp.location.lon
        vessels.add(md)
        stationary_days += sp.duration.total_seconds() / (24.0 * 60.0 * 60.0)
        total_distance_from_shore += sp.mean_distance_from_shore
        total_squared_drift_radius += sp.rms_drift_radius ** 2
    all_destinations = normalized_valid_names(sp.destination for (md, sp) in stationary_periods)

    total_mmsi_count = len(vessels | active_mmsi)

    if n:
        neighbor_s2ids = tuple(s2sphere.CellId.from_token(s2id).get_all_neighbors(ANCHORAGES_S2_SCALE))

        return [AnchoragePoint(
                    mean_location = LatLon(total_lat / n, total_lon / n),
                    total_visits = n, 
                    vessels = frozenset(vessels),
                    mean_distance_from_shore = total_distance_from_shore / n,
                    rms_drift_radius =  math.sqrt(total_squared_drift_radius / n),    
                    top_destinations = tuple(Counter(all_destinations).most_common(dest_limit)),
                    s2id = s2id,
                    neighbor_s2ids = neighbor_s2ids,
                    active_mmsi = active_mmsi_count,
                    total_mmsi = total_mmsi_count,
                    stationary_mmsi_days = stationary_days,
                    active_mmsi_days = active_days
                    )]
    else:
        return []


def AnchoragePt_from_cell_visits(value, dest_limit):
    s2id, visits = value

    n = 0
    total_lat = 0.0
    total_lon = 0.0
    vessels = set()
    total_distance_from_shore = 0.0
    total_squared_drift_radius = 0.0

    for (md, pl) in visits:
        n += 1
        total_lat += pl.location.lat
        total_lon += pl.location.lon
        vessels.add(md)
        total_distance_from_shore += pl.mean_distance_from_shore
        total_squared_drift_radius += pl.rms_drift_radius ** 2
    all_destinations = normalized_valid_names(pl.destination for (md, pl) in visits)

    return AnchoragePoint(
                mean_location = LatLon(total_lat / n, total_lon / n),
                total_visits = n, 
                vessels = frozenset(vessels),
                mean_distance_from_shore = total_distance_from_shore / n,
                rms_drift_radius =  math.sqrt(total_squared_drift_radius / n),    
                top_destinations = tuple(Counter(all_destinations).most_common(dest_limit)),
                s2id = s2id,
                active_mmsi = 0,
                total_mmsi = 0 
                )                  


def find_anchorage_points_cells_2(input, min_unique_vessels_for_anchorage):
    """
    input is a Pipeline object that contains [(md, processed_locations)]

    """
    # (md, stationary_periods) => (md (stationary_locs, )) 
    stationary_periods_by_s2id = (input
        | "addStationaryCellIds" >> beam.FlatMap(lambda (md, processed_locations):
                [(sp.s2id, (md, sp)) for sp in processed_locations.stationary_periods])
        )

    active_points_by_s2id = (input
        | "addActiveCellIds" >> beam.FlatMap(lambda (md, processed_locations):
                [(loc.s2id, (md, loc)) for loc in processed_locations.locations])
        )

    return ((stationary_periods_by_s2id, active_points_by_s2id) 
        | "CogroupOnS2id" >> beam.CoGroupByKey()
        | "createAnchoragePoints" >> beam.FlatMap(AnchoragePts_from_cell_visits2, dest_limit=10)
        | "removeAPointsWFewVessels" >> beam.Filter(lambda x: len(x.vessels) >= min_unique_vessels_for_anchorage)
        )


def find_anchorage_points_cells(input, min_unique_vessels_for_anchorage):
    return (input
        | "addCellIds" >> beam.FlatMap(lambda (md, locations):
                [(pl.s2id, (md, pl)) for pl in locations])
        | "groupByCellIds" >> beam.GroupByKey()
        | "createAnchoragePoints" >> beam.Map(AnchoragePt_from_cell_visits, dest_limit=10)
        | "removeAPointsWFewVessels" >> beam.Filter(lambda x: len(x.vessels) >= min_unique_vessels_for_anchorage)
        )


def anchorage_point_to_json(a_pt):
    return json.dumps({'lat' : a_pt.mean_location.lat, 'lon': a_pt.mean_location.lon,
        'total_visits' : a_pt.total_visits,
        'drift_radius' : a_pt.rms_drift_radius,
        'destinations': a_pt.top_destinations,
        'unique_stationary_mmsi' : len(a_pt.vessels),
        'unique_active_mmsi' : a_pt.active_mmsi,
        'unique_total_mmsi' : a_pt.total_mmsi,
        'active_mmsi_days': a_pt.active_mmsi_days,
        'stationary_mmsi_days': a_pt.stationary_mmsi_days,
        })

             


# AnchoragePoint = namedtuple("AnchoragePoint", ['mean_location',
#                                                'total_visits',
#                                                'vessels',
#                                                'mean_distance_from_shore',
#                                                'rms_drift_radius',
#                                                'top_destinations',
#                                                's2id'])


def datetime_to_text(dt):
    return datetime.datetime.strftime(dt, '%Y-%m-%dT%H:%M:%SZ')

def anchorage_visit_to_json_src(visit):
    latlon = Anchorages.to_LatLon(visit.anchorage)
    s2id = S2_cell_ID(latlon)
    return {'anchorage' : s2id.to_token(), 
            'arrival': datetime_to_text(visit.arrival), 'departure': datetime_to_text(visit.departure)}

def tagged_anchorage_visits_to_json(tagged_visits):
    metadata, visits = tagged_visits
    return json.dumps({'mmsi' : metadata.mmsi, 
        'visits': [anchorage_visit_to_json_src(x) for x in visits]})


class GroupAll(beam.CombineFn):

    def create_accumulator(self):
        return []

    def add_input(self, accumulator, value):
        accumulator.append(value)
        return accumulator

    def merge_accumulators(self, accumulators):
        return list(it.chain(*accumulators))

    def extract_output(self, accumulator):
        return accumulator


# class MergeAdjacentAchoragePointsFn(beam.CombineFn):

#   def create_accumulator(self):
#     return (UnionFind(), {})

#   def add_input(self, accumulator, anchorage_pt):
#     union_find, anchorages_pts_by_id = accumulator
#     anchorages_pts_by_id[anchorage_pt.s2id] = anchorage_pt
#     cellid = s2sphere.CellId.from_token(anchorage_pt.s2id)
#     neighbors = cellid.get_all_neighbors(ANCHORAGES_S2_SCALE)
#     # add s2id if not present
#     union_find[anchorage_pt.s2id]
#     for neighbor_cellid in neighbors:
#         s2id = neighbor_cellid.to_token()
#         if s2id in anchorages_pts_by_id:
#             union_find.union(anchorage_pt.s2id, s2id)
#     return (union_find, anchorages_pts_by_id)

#   def merge_accumulators(self, accumulators):
#     accumiter = iter(accumulators)
#     union_find, anchorages_pts_by_id = next(accumiter)
#     for (uf, apbid) in accumiter:
#         union_find.merge(uf)
#         anchorages_pts_by_id.update(apbid)
#     return (union_find, anchorages_pts_by_id)

#   def extract_output(self, accumulator):
#     union_find, anchorages_pts_by_id = accumulator
#     grouped = {}
#     for s2id in union_find: # TODO may be able to leverage internals of union_find somehow
#         key = union_find[s2id]
#         if key not in grouped:
#             grouped[key] = []
#         grouped[key].append(anchorages_pts_by_id[s2id])

#     for v in grouped.values():
#         v.sort()

#     return grouped.values()


def merge_adjacent_anchorage_points(anchorage_points):
    anchorages_pts_by_id = {S2_cell_ID(ap.mean_location): ap for ap in anchorage_points}

    union_find = UnionFind()

    for ap in anchorage_points:
        neighbors = S2_cell_ID(ap.mean_location).get_all_neighbors(ANCHORAGES_S2_SCALE)
        for n_id in neighbors:
            if n_id in anchorages_pts_by_id:
                nc = anchorages_pts_by_id[n_id]
                # TODO: test this out and activat it?
                # dist = distance(ap.mean_location, nc.mean_location)
                # # Only merge anchorage points where the circles defined by the drift radii overlap.
                # if dist < (ap.rms_drift_radius + nc.rms_drift_radius):
                if True:
                    union_find.union(ap, nc)

    # # Values must be sorted by key to use itertools groupby
    # anchorage_points = list(anchorage_points)
    # anchorage_points.sort(key=lambda x: union_find[x])
    # grouped = it.groupby(anchorage_points, key=lambda x: union_find[x])   
    # return [list(g) for (t, g) in grouped] 

    grouped = {}
    for ap in anchorage_points:
        key = union_find[ap]
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(ap)

    for v in grouped.values():
        v.sort()

    return grouped.values()



class Anchorages(object): 

    @staticmethod
    def to_LatLon(anchorage):
        n = 0
        lat = 0.0
        lon = 0.0
        for ap in anchorage:
            lat += ap.mean_location.lat * len(ap.vessels)
            lon += ap.mean_location.lon * len(ap.vessels)
            n += len(ap.vessels)
        lat /= n
        lon /= n
        return LatLon(lat, lon)

    @staticmethod
    def to_json(anchorage):
        destinations = Counter()
        for ap in anchorage:
            destinations.update(dict(ap.top_destinations))
        latlon = Anchorages.to_LatLon(anchorage)
        s2id = S2_cell_ID(latlon)
        vessels = set()
        for ap in anchorage:
            vessels |= set(ap.vessels)
        return json.dumps({'id' : s2id.to_token(), 'lat' : latlon.lat, 'lon': latlon.lon, 
                            'total_visits': sum(ap.total_visits for ap in anchorage),
                            'unique_mmsi': len(vessels),
                            'destinations': destinations.most_common(10)})




def check_that_pipeline_args_consumed(pipeline):
    options = pipeline.get_all_options(drop_default=True)

    # Some options get translated on the way in (should be a better way to do this...)
    translations = {'--worker_machine_type' : '--machine_type'}
    flags = [translations.get(x, x) for x in pipeline._flags]

    dash_flags = [x for x in flags if x.startswith('-') and x.replace('-', '') not in options]
    if dash_flags:
        print(options)
        print(dash_flags)
        raise ValueError('illegal options specified:\n    {}'.format('\n    '.join(dash_flags)))


preset_runs = {

    'tiny' : ['gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-01-01/001-of-*'],

    '2016' : [
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-01-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-02-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-03-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-04-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-05-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-06-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-07-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-08-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-09-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-10-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-11-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-12-*/*-of-*'
                ],

    'all_years': [
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2012-*-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2013-*-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2014-*-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2015-*-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-*-*/*-of-*',
                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2017-*-*/*-of-*'
                ]
    }

preset_runs['small'] = preset_runs['2016'][-3:]
preset_runs['medium'] = preset_runs['2016'][-6:]



def filter_by_latlon(msg, filters):
    if not is_location_message(msg):
        # Keep non-location messages for destination tagging
        return True
    # Keep any message that falls within a filter region
    lat = msg['lat']
    lon = msg['lon']
    for bounds in filters:
        if ((bounds['min_lat'] <= lat <= bounds['max_lat']) and 
            (bounds['min_lon'] <= lon <= bounds['max_lon'])):
                return True
    # This message is not within any filter region.
    return False


def add_pipeline_defaults(pipeline_args, name):

    defaults = {
        '--project' : 'world-fishing-827',
        '--staging_location' : 'gs://machine-learning-dev-ttl-30d/anchorages/{}/output/staging'.format(name),
        '--temp_location' : 'gs://machine-learning-dev-ttl-30d/anchorages/temp',
        '--setup_file' : './setup.py',
        '--runner': 'DataflowRunner',
        '--max_num_workers' : '200',
        '--job_name': name,
    }

    for name, value in defaults.items():
        if name not in pipeline_args:
            pipeline_args.extend((name, value))


def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('--name', required=True, help='Name to prefix output and job name if not otherwise specified')

    parser.add_argument('--input-patterns', default='all_years',
                                            help='Input file to patterns (comma separated) to process (glob)')
    parser.add_argument('--output',
                                            dest='output',
                                            help='Output file to write results to.')

    parser.add_argument('--latlon-filters',
                                            dest='latlon_filter',
                                            help='newline separated json file containing dicts of min_lat, max_lat, min_lon, max_lon, name')

    known_args, pipeline_args = parser.parse_known_args(argv)

    if known_args.output is None:
        known_args.output = 'gs://machine-learning-dev-ttl-30d/anchorages/{}/output'.format(known_args.name)

    add_pipeline_defaults(pipeline_args, known_args.name)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    check_that_pipeline_args_consumed(pipeline_options)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_options)


    # TODO: Should go in config file or arguments >>>
    min_required_positions = 200 
    stationary_period_min_duration = datetime.timedelta(hours=48)
    stationary_period_max_distance = 0.5 # km
    min_unique_vessels_for_anchorage = 20
    blacklisted_mmsis = [0, 12345]
    anchorage_visit_max_distance = 0.5 # km
    anchorage_visit_min_duration = datetime.timedelta(minutes=60)
    # ^^^

    if known_args.input_patterns in preset_runs:
        input_patterns = preset_runs[known_args.input_patterns]
    else:
        input_patterns = [x.strip() for x in known_args.input_patterns.split(',')]

    ais_input_data_streams = [(p | 'read_{}'.format(i) >> ReadFromText(x)) for (i, x) in  enumerate(input_patterns)]

    ais_input_data = ais_input_data_streams | beam.Flatten()

    if known_args.latlon_filter:
        with open(known_args.latlon_filter) as f:
            latlon_filters = [json.loads(x) for x in f.readlines()]
    else:
        latlon_filters = None


    location_records = read_json_records(ais_input_data, blacklisted_mmsis, latlon_filters)


    grouped_records = (location_records 
        | "GroupByMmsi" >> beam.GroupByKey()
        | "OrderByTimestamp" >> beam.Map(lambda (md, records): (md, sorted(records, key=lambda x: x.timestamp)))
        )

    deduped_records = filter_duplicate_timestamps(grouped_records, min_required_positions)

    thinned_records =   ( deduped_records 
                        | "ThinPoints" >> beam.Map(lambda (md, vlrs): (md, list(thin_points(vlrs)))))

    tagged_records = ( thinned_records 
                     | "TagWithDestinationAndId" >> beam.Map(lambda (md, records): (md, tag_with_destination_and_id(records))))

    processed_records = filter_and_process_vessel_records(tagged_records, stationary_period_min_duration, stationary_period_max_distance) 

    anchorage_points = (find_anchorage_points_cells_2(processed_records, min_unique_vessels_for_anchorage) 
        | 'filterOutInlandPorts' >> beam.Filter(lambda x: not inland_mask.query(x.mean_location)))

    # anchorages = (anchorage_points
    #     | "mergeAnchoragePoints" >> beam.CombineGlobally(GroupAll())
    #     | "groupAnchoragePoints" >> beam.FlatMap(merge_adjacent_anchorage_points))

    (anchorage_points 
        | "convertAPToJson" >> beam.Map(anchorage_point_to_json)
        | "writeAnchoragesPoints" >> WriteToText(known_args.output + '_anchorages_points', file_name_suffix='.json')
    )

    # (anchorages 
    #     | "convertAnToJson" >> beam.Map(Anchorages.to_json)
    #     | 'writeAnchorage' >> WriteToText(known_args.output + '_anchorages', file_name_suffix='.json')
    # )


    result = p.run()
    result.wait_until_finish()

