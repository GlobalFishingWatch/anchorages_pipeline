
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
import yaml
from .port_name_filter import normalized_valid_names
from .union_find import UnionFind
from .sparse_inland_mask import SparseInlandMask
from .distance import distance
from .nearest_port import wpi_finder, geo_finder

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from .records import VesselRecord, InvalidRecord, VesselInfoRecord, VesselLocationRecord


class CreateVesselRecords(beam.PTransform):

    def __init__(self, blacklisted_mmsis):
        self.blacklisted_mmsis = set(blacklisted_mmsis)

    def is_valid(self, item):
        mmsi, rcd = item
        return (not isinstance(rcd, InvalidRecord) and 
                isinstance(mmsi, int) and
                (mmsi not in self.blacklisted_mmsis)) 

    def expand(self, ais_source):
        return (ais_source
            | beam.Map(VesselRecord.from_msg)
            | beam.Filter(self.is_valid)
        )


class CreateTaggedRecords(beam.PTransform):

    def __init__(self, min_required_positions):
        self.min_required_positions = min_required_positions
        self.FIVE_MINUTES = datetime.timedelta(minutes=5)

    def order_by_timestamp(self, item):
        mmsi, records = item
        records = list(records)
        records.sort(key=lambda x: x.timestamp)
        return mmsi, records

    def dedup_by_timestamp(self, item):
        key, source = item
        seen = set()
        sink = []
        for x in source:
            if x.timestamp not in seen:
                sink.append(x)
                seen.add(x.timestamp)
        return (key, sink)

    def long_enough(self, item):
        mmsi, records = item
        return len(records) >= self.min_required_positions

    def thin_records(self, item):
        mmsi, records = item
        last_timestamp = datetime.datetime(datetime.MINYEAR, 1, 1)
        thinned = []
        for rcd in records:
            if (rcd.timestamp - last_timestamp) >= self.FIVE_MINUTES:
                last_timestamp = rcd.timestamp
                thinned.append(rcd)
        return mmsi, thinned

    def tag_records(self, item):
        mmsi, records = item
        dest = ''
        tagged = []
        for rcd in records:
            if isinstance(rcd, VesselInfoRecord):
                # TODO: normalize here rather than later. And cache normalization in dictionary
                dest = rcd.destination
            else:
                tagged.append(rcd._replace(destination=dest))
        return (mmsi, tagged)

    def expand(self, vessel_records):
        return (vessel_records
            | beam.GroupByKey()
            | beam.Map(self.order_by_timestamp)
            | beam.Map(self.dedup_by_timestamp)
            | beam.Filter(self.long_enough)
            | beam.Map(self.thin_records)
            | beam.Map(self.tag_records)
            )



AnchorageVisit = namedtuple('AnchorageVisit',
            ['anchorage', 'arrival', 'departure'])


AnchoragePoint = namedtuple("AnchoragePoint", ['mean_location',
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
                                               'wpi_distance'
                                               'geo_name',
                                               'geo_distance'
                                               ])








def load_config(path):
    with open(path) as f:
        config = yaml.load(f.read())

    anchorage_visit_max_distance = max(config['anchorage_entry_distance_km'],
                                       config['anchorage_exit_distance_km'])

    # Ensure that S2 Cell sizes are large enough that we don't miss ports
    assert (anchorage_visit_max_distance * VISIT_SAFETY_FACTOR < 2 * approx_visit_cell_size)

    return config


LatLon = namedtuple("LatLon", ["lat", "lon"])

def LatLon_from_LatLng(latlng):
    return LatLon(latlng.lat(), latlng.lon())





# # Around (1 km)^2
# ANCHORAGES_S2_SCALE = 13
# Around (0.5 km)^2
ANCHORAGES_S2_SCALE = 14
# Around (16 km)^2
VISITS_S2_SCALE = 9
#
approx_visit_cell_size = 2.0 ** (13 - VISITS_S2_SCALE) 
VISIT_SAFETY_FACTOR = 2.0 # Extra margin factor to ensure we don't miss ports

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

def AnchoragePts_from_cell_visits(value, fishing_vessel_set):
    s2id, (stationary_periods, active_points) = value # tagged_stationary_periods, tagged_active_points

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
        neighbor_s2ids = tuple(s2sphere.CellId.from_token(s2id).get_all_neighbors(ANCHORAGES_S2_SCALE))
        loc = LatLon(total_lat / n, total_lon / n)
        wpi_name, wpi_distance = wpi_finder.find_nearest_port_and_distance(loc)
        geo_name, geo_distance = geo_finder.find_nearest_port_and_distance(loc)

        return [AnchoragePoint(
                    mean_location = loc,
                    total_visits = n, 
                    vessels = frozenset(vessels),
                    fishing_vessels = frozenset(fishing_vessels),
                    rms_drift_radius =  math.sqrt(total_squared_drift_radius / n),    
                    top_destination = Counter(all_destinations).most_common(1)[0],
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
                [(S2_cell_ID(sp.location).to_token(), (md, sp)) 
                        for sp in processed_locations.stationary_periods])
        )

    active_points_by_s2id = (input
        | "addActiveCellIds" >> beam.FlatMap(lambda (md, processed_locations):
                [(S2_cell_ID(rcd.location).to_token(), (md, rcd)) 
                        for rcd in processed_locations.active_records])
        )

    return ((stationary_periods_by_s2id, active_points_by_s2id) 
        | "CogroupOnS2id" >> beam.CoGroupByKey()
        | "createAnchoragePoints" >> beam.FlatMap(AnchoragePts_from_cell_visits, fishing_vessel_set=fishing_vessels)
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





