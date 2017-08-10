from __future__ import absolute_import, print_function, division

import argparse
import logging
import re
import ujson as json
import datetime
from collections import namedtuple, Counter, defaultdict
import itertools as it
import math
import pickle
import s2sphere
import bisect

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

# TODO: Try? to switch to s2sphere LatLon



# from skimage import io
# import numpy as np
# imarray = io.imread("dist2coast_1deg_ocean_negative-land_v3.tiff")
# np.save("inland.npy", imarray == -10)


class SparseInlandMask(object):

    def __init__(self):
        with open("sparse_inland.pickle") as f:
            mask_info = pickle.load(f)
        self.mask_data = mask_info['data']
        self.MAX_LAT = mask_info['max_lat']
        self.MIN_LAT = mask_info['min_lat']
        self.MAX_LON = mask_info['max_lon']
        self.MIN_LON = mask_info['min_lon']
        self._dlat = (self.MAX_LAT - self.MIN_LAT) / mask_info['n_lat']
        self._dlon = (self.MAX_LON - self.MIN_LON) / mask_info['n_lon']

    def query(self, loc):
        lat, lon = loc
        i = (self.MAX_LAT - lat) // self._dlat
        j = (lon - self.MIN_LON) // self._dlon
        ndx = bisect.bisect_right(self.mask_data[int(i)], j)
        return ndx & 1

    def checked_query(self, loc):
        lat, lon = loc
        assert self.MIN_LAT <= lat < self.MAX_LAT
        assert self.MIN_LON <= lat < self.MAX_LON
        return self.query(loc)

inland_mask = SparseInlandMask()


"""UnionFind.py

Union-find data structure. Based on Josiah Carlson's code,
http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/215912
with significant additional changes by D. Eppstein.
"""

class UnionFind(object):
    """Union-find data structure.

    Each unionFind instance X maintains a family of disjoint sets of
    hashable objects, supporting the following two methods:

    - X[item] returns a name for the set containing the given item.
      Each set is named by an arbitrarily-chosen one of its members; as
      long as the set remains unchanged it will keep the same name. If
      the item is not yet part of a set in X, a new singleton set is
      created for it.

    - X.union(item1, item2, ...) merges the sets containing each item
      into a single larger set.  If any item is not yet part of a set
      in X, it is added to X as one of the members of the merged set.
    """

    def __init__(self):
        """Create a new empty union-find structure."""
        self.weights = {}
        self.parents = {}

    def __getitem__(self, object):
        """Find and return the name of the set containing the object."""

        # check for previously unknown object
        if object not in self.parents:
            self.parents[object] = object
            self.weights[object] = 1
            return object

        # find path of objects leading to the root
        path = [object]
        root = self.parents[object]
        while root != path[-1]:
            path.append(root)
            root = self.parents[root]

        # compress the path and return
        for ancestor in path:
            self.parents[ancestor] = root
        return root
        
    def __iter__(self):
        """Iterate through all items ever found or unioned by this structure."""
        return iter(self.parents)

    def union(self, *objects):
        """Find the sets containing the objects and merge them all."""
        roots = [self[x] for x in objects]
        heaviest = max([(self.weights[r],r) for r in roots])[1]
        for r in roots:
            if r != heaviest:
                self.weights[heaviest] += self.weights[r]
                self.parents[r] = heaviest

    def merge(self, *union_finds):
        """Merge additional UnionFinds into this one"""
        for uf in union_finds:
            for child in uf:
                # For each node in accum, look up parents in both
                # base and accum. Then merge the parents.
                p0 = self[child]
                p1 = uf[child]
                self.union(p0, p1)



def GroupAll(p, name="GroupAll"):
        return (p 
            | name + "AddKey" >> beam.Map(lambda x: (0, x))
            | name + "Group" >> beam.GroupByKey()
            | name + "RemoveKey" >> beam.Map(lambda (_, x): x)
            )



VesselMetadata = namedtuple('VesselMetadata', ['mmsi'])

def VesselMetadata_from_msg(msg):
    return VesselMetadata(msg['mmsi'])


VesselLocationRecord = namedtuple('VesselLocationRecord',
            ['timestamp', 'location', 'distance_from_shore', 'speed', 'course'])

TaggedVesselLocationRecord = namedtuple('TaggedVesselLocationRecord',
            ['destination', 'timestamp', 'location', 'distance_from_shore', 'speed', 'course'])

VesselInfoRecord = namedtuple('VesselInfoRecord',
            ['timestamp', 'destination'])


AnchorageVisit = namedtuple('AnchorageVisit',
            ['anchorage', 'arrival', 'departure'])


def Records_from_msg(msg, blacklisted_mmsis):

    mmsi = msg.get('mmsi')
    if not isinstance(mmsi, int) or (mmsi in blacklisted_mmsis):
        return []

    metadata = VesselMetadata_from_msg(msg)

    if is_location_message(msg):
        return [(metadata, VesselLocationRecord(
            datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ'), 
            LatLon(msg['lat'], msg['lon']), 
            msg['distance_from_shore'] / 1000.0,
            round(msg['speed'], 1),
            msg['course']))]
    elif msg.get('destination') not in set(['', None]):
        return [(metadata, VesselInfoRecord(
            datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ'),
            msg['destination']
            ))]
    else:
        return []



AnchoragePoint = namedtuple("AnchoragePoint", ['mean_location',
                                               'vessels',
                                               'mean_distance_from_shore',
                                               'mean_drift_radius',
                                               'top_destinations'])




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

def tag_with_destination(records):
    """filter out info messages and use them to tag subsequent records"""
    dest = ''
    new = []
    for rcd in records:
        if isinstance(rcd, VesselInfoRecord):
            dest = rcd.destination
        else:
            new.append(TaggedVesselLocationRecord(dest, *rcd)) 
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
        'mean_drift_radius', 'destination'])

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
                    mean_drift_radius = sum(distance(x.location, mean_location) for x in current_period)

                    stationary_periods.append(StationaryPeriod(mean_location, duration, 
                                                               mean_distance_from_shore, mean_drift_radius,
                                                               first_vr.destination))
                else:
                    without_stationary_periods.extend(current_period)
                current_period = []
        current_period.append(vr)
    without_stationary_periods.extend(current_period)

    return ProcessedLocations(without_stationary_periods, stationary_periods) 






def filter_and_process_vessel_records(input, stationary_period_min_duration, stationary_period_max_distance):
    return input | beam.Map( lambda (metadata, records):
                        (metadata, 
                         remove_stationary_periods(list(thin_points(records)), stationary_period_min_duration, stationary_period_max_distance)))


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


def find_destinations(seq, limit):
    return tuple(Counter(x.destination for x in seq if x.destination not in set([''])).most_common(limit))


bogus_destinations = set([''])

def AnchoragePt_from_cell_visits(value, dest_limit):
    cell, visits = value

    n = 0
    total_lat = 0.0
    total_lon = 0.0
    vessels = set()
    total_distance_from_shore = 0.0
    total_drift_radius = 0.0
    all_destinations = []

    for (md, pl) in visits:
        n += 1
        total_lat += pl.location.lat
        total_lon += pl.location.lon
        vessels.add(md)
        total_distance_from_shore += pl.mean_distance_from_shore
        total_drift_radius += pl.mean_drift_radius
        dest = pl.destination
        if dest not in bogus_destinations:
            all_destinations.append(dest)

    return AnchoragePoint(
                mean_location = LatLon(total_lat / n, total_lon / n),
                vessels = frozenset(vessels),
                mean_distance_from_shore = total_distance_from_shore / n,
                mean_drift_radius =  total_drift_radius / n,    
                top_destinations = tuple(Counter(all_destinations).most_common(dest_limit))           
                )    



def find_anchorage_point_cells(input, min_unique_vessels_for_anchorage):
    return (input
        | "addCellIds" >> beam.FlatMap(lambda (md, locations):
                [(S2_cell_ID(pl.location), (md, pl)) for pl in locations.stationary_periods])
        | "groupByCellIds" >> beam.GroupByKey()
        | "createAnchoragePoints" >> beam.Map(AnchoragePt_from_cell_visits, dest_limit=10)
        # | "createAnchoragePoints" >> beam.Map(lambda (cell, visits):
        #     AnchoragePoint(
        #         mean_location = LatLon_mean(pl.location for (md, pl) in visits),
        #         vessels = frozenset(md for (md, pl) in visits),
        #         mean_distance_from_shore = mean(pl.mean_distance_from_shore for (md, pl) in visits),
        #         mean_drift_radius = mean(pl.mean_drift_radius for (md, pl) in visits),    
        #         top_destinations = find_destinations((pl for (md, pl) in visits), limit=10)            
        #         )
        #     )
        | "removeAPointsWFewVessels" >> beam.Filter(lambda x: len(x.vessels) >= min_unique_vessels_for_anchorage)
        )


def anchorage_point_to_json(a_pt):
    return json.dumps({'lat' : a_pt.mean_location.lat, 'lon': a_pt.mean_location.lon})


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
    s2id = S2_cell_ID(anchorage_pt.mean_location)
    anchorages_pts_by_id[s2id] = anchorage_pt
    neighbors = S2_cell_ID(anchorage_pt.mean_location).get_all_neighbors(ANCHORAGES_S2_SCALE)
    for n_id in neighbors:
        if n_id in anchorages_pts_by_id:
            nc = anchorages_pts_by_id[n_id]
            union_find.union(anchorage_pt, nc)

  def merge_accumulators(self, accumulators):
    base, anchorages_pts_by_id = accumulators[0]
    base.merge(*[uf for (uf, _) in accumulators[1:]])
    for (_, apid) in accumulators[1:]:
        anchorages_pts_by_id.update(apid)
    return (base, anchorages_pts_by_id)

  def extract_output(self, union_find):
    grouped = {}
    for ap in union_find: # TODO may be able to leverage internals of union_find somehow
        key = union_find[ap]
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(ap)

    for v in grouped.values():
        v.sort()

    return grouped.values()


# # TODO, think about building a custom accumulator on top
# # of beam.CombineFn so that we don't need to do combine all values

def merge_adjacent_anchorage_points(anchorage_points):
    anchorages_pts_by_id = {S2_cell_ID(ap.mean_location): ap for ap in anchorage_points}

    union_find = UnionFind()

    for ap in anchorage_points:
        neighbors = S2_cell_ID(ap.mean_location).get_all_neighbors(ANCHORAGES_S2_SCALE)
        for n_id in neighbors:
            if n_id in anchorages_pts_by_id:
                nc = anchorages_pts_by_id[n_id]
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

    # @staticmethod
    # def find_visits(input, anchorages, min_duration):
    #     anchorages =  pvalue.AsList(anchorages)   

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
        return json.dumps({'id' : s2id.to_token(), 'lat' : latlon.lat, 'lon': latlon.lon, 
                            'destinations': destinations.most_common(10)})




def get_cap_covering_cells(latlon, max_radius_km, s2_level):
    cap_radius_on_unit_sphere = max_radius_km / EARTH_RADIUS
    coverer = s2sphere.RegionCoverer()
    coverer.min_level = coverer.max_level = s2_level

    # S2 cap requires an axis (location on unit sphere) and the height of the cap (the cap is
    # a planar cut on the unit sphere). The cap height is 1 - (sqrt(r^2 - a^2)/r) where r is
    # the radius of the circle (1.0 after we've normalized) and a is the radius of the cap itself.
    cap_axis = s2sphere.LatLng.from_degrees(latlon.lat, latlon.lon).normalized().to_point()
    cap_height = 1.0 - math.sqrt(1.0 - cap_radius_on_unit_sphere ** 2)
    cap = s2sphere.Cap.from_axis_height(cap_axis, cap_height)

    cover_cells = coverer.get_covering(cap)

    assert all((x.level() == s2_level for x in cover_cells))

    return cover_cells


def create_cell_map(anchorages, max_radius, level):
    cell_map = defaultdict(list)
    for anch in anchorages:
        for ap in anch:
            cap_cells = get_cap_covering_cells(ap.mean_location, max_radius, level)
            for cellid in cap_cells:
                cell_map[cellid].append(ap)
    return dict(cell_map)




def lookup_nearby(location, max_radius, level, cell_map):
    cap_cells = get_cap_covering_cells(location, max_radius, level)
    all_nearby_cells = it.chain.from_iterable((cell_map.get(x, ()) for x in cap_cells))

    all_nearby_cells = all_nearby_cells

    values = [(distance(location, x.mean_location), x) for x in all_nearby_cells]
    values.sort(key=lambda x: x[0])
    return values


def find_anchorage_visits(value, anchorages, max_distance, min_visit_duration):
    cell_map = create_cell_map(anchorages, max_distance, ANCHORAGES_S2_SCALE)

    (metadata, locations) = value
    anchorages_by_anchorage_pts = {}
    for anchorage in anchorages:
        for anchorage_pt in anchorage:
            anchorages_by_anchorage_pts[anchorage_pt] = anchorage

    raw_visits = []
    for loc in locations:
        a_pts = lookup_nearby(loc.location, max_distance, ANCHORAGES_S2_SCALE, cell_map) 
        if len(a_pts) > 0:
            raw_visits.append(
                AnchorageVisit(anchorages_by_anchorage_pts[a_pts[0][1]],
                           loc.timestamp,
                           loc.timestamp))

    visits = []
    if raw_visits:
        visits.append(raw_visits[0])
        for rv in raw_visits[1:]:
            last = visits[-1]
            if last.anchorage == rv.anchorage:
                visits[-1] = AnchorageVisit(last.anchorage, last.arrival, rv.departure)
            else:
                visits.append(rv)

    visits = [x for x in visits if (x.departure - x.arrival) > min_visit_duration]

    return (metadata, visits)


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


def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline.


    python -m anchorages \
        --project world-fishing-827 \
        --job_name test-anchorages-visits-accum-2 \
        --runner DataflowRunner \
        --staging_location gs://world-fishing-827/scratch/timh/output/staging \
        --temp_location gs://world-fishing-827/scratch/timh/temp \
        --requirements_file requirements.txt \
        --max_num_workers 200 \


    python -m anchorages \
        --project world-fishing-827 \
        --job_name test-anchorages-visits-accum-2 \
        --runner DataflowRunner \
        --staging_location gs://world-fishing-827/scratch/timh/output/staging \
        --temp_location gs://world-fishing-827/scratch/timh/temp \
        --requirements_file requirements.txt \
        --max_num_workers 50 \
        --worker_machine_type n1-highmem-4 


    python -m anchorages \
        --input-pattern gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-01-01/001-of-* 


    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-patterns',
                                        default=
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2012-*-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2013-*-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2014-*-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2015-*-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-*-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2017-*-*/*-of-*',
                                                #
                                                # 'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-01-*/*-of-*,'
                                                # 'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-02-*/*-of-*,'
                                                # 'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-03-*/*-of-*,'
                                                # 'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-04-*/*-of-*,'
                                                # 'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-05-*/*-of-*,'
                                                # 'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-06-*/*-of-*,'
                                                # 'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-07-*/*-of-*,'
                                                # 'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-08-*/*-of-*,'
                                                # 'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-09-*/*-of-*,'
                                                # 'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-10-*/*-of-*,'
                                                # 'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-11-*/*-of-*,'
                                                # 'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-12-*/*-of-*',
                                            help='Input file to patterns (comma separated) to process (glob)')
    parser.add_argument('--output',
                                            dest='output',
                                            default='gs://world-fishing-827/scratch/timh/output/test_anchorages',
                                            help='Output file to write results to.')

    parser.add_argument('--skip-visits',    action='store_true',
                                            help='Skip generating vists.')


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
    input_patterns = [x.strip() for x in known_args.input_patterns.split(',')]

    ais_input_data_streams = [(p | 'read_{}'.format(i) >> ReadFromText(x)) for (i, x) in  enumerate(input_patterns)]

    ais_input_data = ais_input_data_streams | beam.Flatten()

    location_records = read_json_records(ais_input_data, blacklisted_mmsis)

    grouped_records = (location_records 
        | "GroupByMmsi" >> beam.GroupByKey()
        | "OrderByTimestamp" >> beam.Map(lambda (md, records): (md, sorted(records, key=lambda x: x.timestamp)))
        | "TagWithDestination" >> beam.Map(lambda (md, records): (md, tag_with_destination(records))))


    deduped_records = filter_duplicate_timestamps(grouped_records, min_required_positions)

    processed = filter_and_process_vessel_records(deduped_records, stationary_period_min_duration, stationary_period_max_distance)

    anchorage_points = (find_anchorage_point_cells(processed, min_unique_vessels_for_anchorage) |
                        beam.Filter(lambda x: not inland_mask.query(x.mean_location)))


    anchorages = (GroupAll(anchorage_points, "GroupAllAnchorages")
        | "MergeAdjacentPoints" >> beam.FlatMap(merge_adjacent_anchorage_points))

    # anchorages = (anchorage_points
    #     | "mergeAnchoragePoints" >> beam.CombineGlobally(MergeAdjacentAchoragePointsFn())
    #     | "reparallelizeAnchorages" >> beam.FlatMap(lambda x: x)
    # )

    (anchorage_points 
        | "convertAPToJson" >> beam.Map(anchorage_point_to_json)
        | "writeAnchoragesPoints" >> WriteToText(known_args.output + '_anchorages_points', file_name_suffix='.json')
    )

    (anchorages 
        | "convertAnToJson" >> beam.Map(Anchorages.to_json)
        | 'writeAnchorage' >> WriteToText(known_args.output + '_anchorages', file_name_suffix='.json')
    )

    if not known_args.skip_visits:
        anchorages_list = beam.pvalue.AsList(anchorages)
        (deduped_records 
            | "findAnchorageVisits" >> beam.Map(find_anchorage_visits, anchorages_list, 
                                                 anchorage_visit_max_distance, anchorage_visit_min_duration)
            | "convertToJson" >> beam.Map(tagged_anchorage_visits_to_json)
            | "writeAnchorageVisits" >> WriteToText(known_args.output + '_anchorages')
        )


    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
