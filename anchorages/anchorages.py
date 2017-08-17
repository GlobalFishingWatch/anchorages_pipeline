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
            ['destination', 's2id', 'timestamp', 'location', 'distance_from_shore', 'speed', 'course'])

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
                                               's2id'])



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


def read_json_records(input, blacklisted_mmsis):
    return (input 
        | "Parse" >> beam.Map(json.loads)
        | "CreateLocationRecords" >> beam.FlatMap(Records_from_msg, blacklisted_mmsis)
        | "FilterOutBadValues" >> beam.Filter(lambda (md, lr): is_not_bad_value(lr))
        )

def tag_with_destination_and_id(records):
    """filter out info messages and use them to tag subsequent records"""
    dest = ''
    new = []
    for rcd in records:
        if isinstance(rcd, VesselInfoRecord):
            # TODO: normalize here rather than later. And cache normalization in dictionary
            dest = rcd.destination
        else:
            new.append(TaggedVesselLocationRecord(dest, S2_cell_ID(rcd.location).to_token(), 
                                                  *rcd)) 
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
    # TODO(alexwilson): Tim points out that leaves vessels sitting around for t - delta looking
    # significantly different from those sitting around for t + delta. Consider his scheme of just
    # cropping all excess time over the threshold instead.

    without_stationary_periods = []
    stationary_periods = []
    current_period = []

    for vr in records:
        if current_period:
            first_vr = current_period[0]
            if distance(vr.location, first_vr.location) > stationary_period_max_distance:
                if vr.timestamp - first_vr.timestamp > stationary_period_min_duration:
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
    return  ( input 
            | "splitIntoStationaryNonstationaryPeriods" >> beam.Map( lambda (metadata, records):
                        (metadata, 
                         remove_stationary_periods(records, stationary_period_min_duration, stationary_period_max_distance)))

            | "ExtractStationaryPeriods" >> beam.Map(lambda (md, x): (md, x.stationary_periods))
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
                s2id = s2id       
                )    



def find_anchorage_point_cells(input, min_unique_vessels_for_anchorage):
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
        'unique_mmsi' : len(a_pt.vessels),
        'destinations': a_pt.top_destinations})


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


class MergeAdjacentAchoragePointsFn(beam.CombineFn):

  def create_accumulator(self):
    return (UnionFind(), {})

  def add_input(self, accumulator, anchorage_pt):
    union_find, anchorages_pts_by_id = accumulator
    anchorages_pts_by_id[anchorage_pt.s2id] = anchorage_pt
    cellid = s2sphere.CellId.from_token(anchorage_pt.s2id)
    neighbors = cellid.get_all_neighbors(ANCHORAGES_S2_SCALE)
    # add s2id if not present
    union_find[anchorage_pt.s2id]
    for neighbor_cellid in neighbors:
        s2id = neighbor_cellid.to_token()
        if s2id in anchorages_pts_by_id:
            union_find.union(anchorage_pt.s2id, s2id)
    return (union_find, anchorages_pts_by_id)

  def merge_accumulators(self, accumulators):
    accumiter = iter(accumulators)
    union_find, anchorages_pts_by_id = next(accumiter)
    for (uf, apbid) in accumiter:
        union_find.merge(uf)
        anchorages_pts_by_id.update(apbid)
    return (union_find, anchorages_pts_by_id)

  def extract_output(self, accumulator):
    union_find, anchorages_pts_by_id = accumulator
    grouped = {}
    for s2id in union_find: # TODO may be able to leverage internals of union_find somehow
        key = union_find[s2id]
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(anchorages_pts_by_id[s2id])

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




def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-patterns', default='all_years',
                                            help='Input file to patterns (comma separated) to process (glob)')
    parser.add_argument('--output',
                                            dest='output',
                                            default='gs://world-fishing-827/scratch/timh/output/test_anchorages_2',
                                            help='Output file to write results to.')


    known_args, pipeline_args = parser.parse_known_args(argv)

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

    location_records = read_json_records(ais_input_data, blacklisted_mmsis)

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


    anchorage_points = (find_anchorage_point_cells(processed_records, min_unique_vessels_for_anchorage) |
                        beam.Filter(lambda x: not inland_mask.query(x.mean_location)))

    anchorages = (anchorage_points
        | "mergeAnchoragePoints" >> beam.CombineGlobally(MergeAdjacentAchoragePointsFn())
        | "reparallelizeAnchorages" >> beam.FlatMap(lambda x: x)
    )

    (anchorage_points 
        | "convertAPToJson" >> beam.Map(anchorage_point_to_json)
        | "writeAnchoragesPoints" >> WriteToText(known_args.output + '_anchorages_points', file_name_suffix='.json')
    )

    (anchorages 
        | "convertAnToJson" >> beam.Map(Anchorages.to_json)
        | 'writeAnchorage' >> WriteToText(known_args.output + '_anchorages', file_name_suffix='.json')
    )


    result = p.run()
    result.wait_until_finish()

