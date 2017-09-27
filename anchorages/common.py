
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
from .distance import distance
from .nearest_port import port_finder, AnchorageFinder, BUFFER_KM as VISIT_BUFFER_KM

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from .records import VesselRecord, VesselInfoRecord, VesselLocationRecord


# VesselMetadata = namedtuple('VesselMetadata', ['mmsi'])


class CreateVesselRecords(beam.PTransform):

    def __init__(self, blacklisted_mmsis):
        self.blacklisted_mmsis = blacklisted_mmsis

    def from_msg(self, msg):
        obj = VesselRecord.from_msg(msg)
        if (obj is None) or (obj[0] in self.blacklisted_mmsis):
            return []
        else:
            return [obj]

    def expand(self, ais_source):
        return (ais_source
            | beam.FlatMap(self.from_msg)
        )




def Records_from_msg(msg, blacklisted_mmsis):
    obj = VesselRecord.from_msg(msg)
    if obj is None or obj[0] in blacklisted_mmsis:
        return []
    else:
        return [obj]


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


TaggedVesselLocationRecord = namedtuple('TaggedVesselLocationRecord',
            ['destination', 's2id', 'is_new_id', 'timestamp', 'location'])




AnchorageVisit = namedtuple('AnchorageVisit',
            ['anchorage', 'arrival', 'departure'])





AnchoragePoint = namedtuple("AnchoragePoint", ['mean_location',
                                               'total_visits',
                                               'vessels',
                                               'fishing_vessels',
                                               'rms_drift_radius',
                                               'top_destinations',
                                               's2id',
                                               'neighbor_s2ids',
                                               'active_mmsi',
                                               'total_mmsi',
                                               'stationary_mmsi_days',
                                               'stationary_fishing_mmsi_days',
                                               'active_mmsi_days',
                                               'port_name',
                                               'port_distance'
                                               ])



def is_location_message(msg):
    return (
                msg.get('lat') is not None  and 
                msg.get('lon') is not None
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
                                                  *rcd[:-1])) 
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


StationaryPeriod = namedtuple("StationaryPeriod", ['location', 'start_time', 'duration',
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
                    rms_drift_radius = math.sqrt(sum(distance(x.location, mean_location)**2 for x in current_period) / num_points)
                    stationary_periods.append(StationaryPeriod(mean_location, 
                                                               first_vr.timestamp,
                                                               duration, 
                                                               rms_drift_radius,
                                                               first_vr.destination,
                                                               s2id=S2_cell_ID(mean_location).to_token()))
                else:
                    without_stationary_periods.extend(current_period)
                current_period = []
        current_period.append(vr)
    without_stationary_periods.extend(current_period)

    return ProcessedLocations(without_stationary_periods, stationary_periods) 


#  TODO: defunctionify
def filter_and_process_vessel_records(input, stationary_period_min_duration, stationary_period_max_distance, prefix=''):
    return ( input 
            | prefix + "splitIntoStationaryNonstationaryPeriods" >> beam.Map( lambda (metadata, records):
                        (metadata, 
                         remove_stationary_periods(records, stationary_period_min_duration, stationary_period_max_distance)))
            )

# # Around (1 km)^2
# ANCHORAGES_S2_SCALE = 13
# Around (0.5 km)^2
ANCHORAGES_S2_SCALE = 14
# Around (16 km)^2
VISITS_S2_SCALE = 9
#
# TODO: revisit
approx_visit_cell_size = 2.0 ** (13 - VISITS_S2_SCALE) 
VISIT_SAFETY_FACTOR = 2.0 # Extra margin factor to ensure VISIT_BUFFER_KM is large enough

def S2_cell_ID(loc, scale=ANCHORAGES_S2_SCALE):
    ll = s2sphere.LatLng.from_degrees(loc.lat, loc.lon)
    return s2sphere.CellId.from_lat_lng(ll).parent(scale)

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



bogus_destinations = set([''])

def AnchoragePts_from_cell_visits(value, dest_limit, fishing_vessel_set):
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

    for (md, sp) in stationary_periods:
        n += 1
        total_lat += sp.location.lat
        total_lon += sp.location.lon
        vessels.add(md)
        stationary_days += sp.duration.total_seconds() / (24.0 * 60.0 * 60.0)
        if md.mmsi in fishing_vessel_set:
            fishing_vessels.add(md)
            stationary_fishing_days += sp.duration.total_seconds() / (24.0 * 60.0 * 60.0)
        total_squared_drift_radius += sp.rms_drift_radius ** 2
    all_destinations = normalized_valid_names(sp.destination for (md, sp) in stationary_periods)

    total_mmsi_count = len(vessels | active_mmsi)

    if n:
        neighbor_s2ids = tuple(s2sphere.CellId.from_token(s2id).get_all_neighbors(ANCHORAGES_S2_SCALE))
        loc = LatLon(total_lat / n, total_lon / n)
        port_name, port_distance = port_finder.find_nearest_port_and_distance(loc)

        return [AnchoragePoint(
                    mean_location = loc,
                    total_visits = n, 
                    vessels = frozenset(vessels),
                    fishing_vessels = frozenset(fishing_vessels),
                    rms_drift_radius =  math.sqrt(total_squared_drift_radius / n),    
                    top_destinations = tuple(Counter(all_destinations).most_common(dest_limit)),
                    s2id = s2id,
                    neighbor_s2ids = neighbor_s2ids,
                    active_mmsi = active_mmsi_count,
                    total_mmsi = total_mmsi_count,
                    stationary_mmsi_days = stationary_days,
                    stationary_fishing_mmsi_days = stationary_fishing_days,
                    active_mmsi_days = active_days,
                    port_name = port_name,
                    port_distance = port_distance
                    )]
    else:
        return []



def find_anchorage_points(input, min_unique_vessels_for_anchorage, fishing_vessels):
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
        | "createAnchoragePoints" >> beam.FlatMap(AnchoragePts_from_cell_visits, dest_limit=10, fishing_vessel_set=fishing_vessels)
        | "removeAPointsWFewVessels" >> beam.Filter(lambda x: len(x.vessels) >= min_unique_vessels_for_anchorage)
        )




def anchorage_point_to_json(a_pt):
    return json.dumps({'lat' : a_pt.mean_location.lat, 'lon': a_pt.mean_location.lon,
        'total_visits' : a_pt.total_visits,
        'drift_radius' : a_pt.rms_drift_radius,
        'destinations': a_pt.top_destinations,
        'unique_stationary_mmsi' : len(a_pt.vessels),
        'unique_stationary_fishing_mmsi' : len(a_pt.fishing_vessels),
        'unique_active_mmsi' : a_pt.active_mmsi,
        'unique_total_mmsi' : a_pt.total_mmsi,
        'active_mmsi_days': a_pt.active_mmsi_days,
        'stationary_mmsi_days': a_pt.stationary_mmsi_days,
        'stationary_fishing_mmsi_days': a_pt.stationary_fishing_mmsi_days,
        'port_name': a_pt.port_name,
        'port_distance': a_pt.port_distance,
        's2id' : a_pt.s2id
        })



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





