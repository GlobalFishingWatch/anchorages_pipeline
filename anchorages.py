from __future__ import absolute_import, print_function, division

import argparse
import logging
import re
import json
import numpy as np
import datetime
from collections import namedtuple, Counter
import itertools as it
import math
import s2sphere

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



# from skimage import io
# import numpy as np
# imarray = io.imread("dist2coast_1deg_ocean_negative-land_v3.tiff")
# np.save("inland.npy", imarray == -10)


class InlandMask(object):

    MIN_LON = -180.0044166
    MAX_LON = 180.0044166
    MIN_LAT = -90.0022083
    MAX_LAT = 90.0022083

    def __init__(self):
        self.mask = np.load("inland.npy")
        self.nlat, self.nlon = self.mask.shape
        self.dlat = (self.MAX_LAT - self.MIN_LAT) / self.nlat
        self.dlon = (self.MAX_LON - self.MIN_LON) / self.nlon

    def __getitem__(self, loc):
        lat, lon = loc
        i = (self.MAX_LAT - lat) // self.dlat
        j = (lon - self.MIN_LON) // self.dlon
        print(i, j, self.mask[int(i), int(j)])
        return self.mask[int(i), int(j)]

inland_mask = InlandMask()


"""UnionFind.py

Union-find data structure. Based on Josiah Carlson's code,
http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/215912
with significant additional changes by D. Eppstein.
"""

class UnionFind:
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

def VesselLocationRecord_from_msg(msg):
    return VesselLocationRecord(
        datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ'), 
        LatLon(msg['lat'], msg['lon']), 
        msg['distance_from_shore'] / 1000.0,
        round(msg['speed'], 1),
        msg['course'])


AnchoragePoint = namedtuple("AnchoragePoint", ['mean_location',
                                               'vessels',
                                               'mean_distance_from_shore',
                                               'mean_drift_radius'])


def is_location_message(msg):
    return ('mmsi' in msg and isinstance(msg['mmsi'], int) and
            'lat' in msg and 
            'lon' in msg and
            'speed' in msg and
            'distance_from_shore' in msg and
            'course' in msg)

def is_not_bad_value(lr):
    return (
    -90 <= lr.location.lat <= 90 and
    -180 <= lr.location.lon <= 180 and
    0 <= lr.distance_from_shore <= 20000 and
    0 <= lr.course < 360 
    )


def read_json_records(input, blacklisted_mmsis):
    return (input 
        | "Parse" >> beam.Map(json.loads)
        | "SelectLocationMessages" >> beam.Filter(is_location_message)
        | "CreateLocationRecords" >> beam.Map(lambda msg: (
            VesselMetadata_from_msg(msg), VesselLocationRecord_from_msg(msg)))
        | "FilterOutBlacklistedMMSI" >> beam.Filter(lambda (md, lr): md.mmsi not in blacklisted_mmsis)
        | "FilterOutBadValues" >> beam.Filter(lambda (md, lr): is_not_bad_value(lr))
        )

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

StationaryPeriod = namedtuple("StationaryPeriod", ['location', 'duration', 'mean_distance_from_shore', 'mean_drift_radius'])

LatLon = namedtuple("LatLon", ["lat", "lon"])

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
                                                               mean_distance_from_shore, mean_drift_radius))
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


# def find_destinations(seq, limit):
#     # TODO: ideally, we group by mmsi and take the first for each visit
#     # Might need to dig into stationary periods code to do that right and add to metadata
#     return Counter(x.destination for x in seq if x.destination not in set([''])).most_common(limit)


def find_anchorage_point_cells(input, min_unique_vessels_for_anchorage):
    return (input
        | beam.FlatMap(lambda (md, locations):
                [(S2_cell_ID(pl.location), (md, pl)) for pl in locations.stationary_periods])
        | beam.GroupByKey()
        | beam.Map(lambda (cell, visits):
            AnchoragePoint(
                mean_location = LatLon_mean(pl.location for (md, pl) in visits),
                vessels = frozenset(md for (md, pl) in visits),
                mean_distance_from_shore = mean(pl.mean_distance_from_shore for (md, pl) in visits),
                mean_drift_radius = mean(pl.mean_drift_radius for (md, pl) in visits),    
                # top_destinations = find_destinations((pl for (md, pl) in visits), limit=10)            
                )
            )
        | beam.Filter(lambda x: len(x.vessels) >= min_unique_vessels_for_anchorage)
        )


def anchorage_point_to_json(a_pt):
    return {'lat' : a_pt.mean_location.lat, 'lon': a_pt.mean_location.lon}


# def anchorange_to_json(anchorage):
#     n = 0
#     lat = 0.0
#     lon = 0.0
#     # destinations = Counter()
#     for ap in anchorage:
#         lat += ap.mean_location.lat * len(ap.vessels)
#         lon += ap.mean_location.lon * len(ap.vessels)
#         # counter.update(dict(ap.top_destinations))
#         n += len(ap.vessels)
#     lat /= n
#     lon /= n
#     return {'lat' : lat, 'lon': lon}



# TODO, think about building a custom accumulator on top
# of beam.CombineFn so that we don't need to do combine all values

def merge_adjacent_anchorage_points(anchorage_points):
    anchorages_by_id = {S2_cell_ID(ap.mean_location): ap for ap in anchorage_points}

    union_find = UnionFind()

    for ap in anchorage_points:
        # TODO: consider using get_all_neighbors
        neighbors = S2_cell_ID(ap.mean_location).get_edge_neighbors()
        for x in neighbors:
            id = x.to_token()
            if id in anchorages_by_id:
                nc = anchorages_by_id[id]
                union_find.union(ap, nc)

    # Values must be sorted by key to use itertools groupby
    anchorage_points = list(anchorage_points)
    anchorage_points.sort(key=lambda x: union_find[x])
    grouped = it.groupby(anchorage_points, key=lambda x: union_find[x])   
    return [list(g) for (t, g) in grouped] 








class Anchorages(object):



    @staticmethod
    def find_visits(input, anchorages, min_duration):
        anchorages =  pvalue.AsList(anchorages)   

    @staticmethod
    def to_json(anchorage):
        n = 0
        lat = 0.0
        lon = 0.0
        # destinations = Counter()
        for ap in anchorage:
            lat += ap.mean_location.lat * len(ap.vessels)
            lon += ap.mean_location.lon * len(ap.vessels)
            # counter.update(dict(ap.top_destinations))
            n += len(ap.vessels)
        lat /= n
        lon /= n
        return json.dumps({'lat' : lat, 'lon': lon})


 # def findAnchorageVisits(
 #      locationEvents: SCollection[(VesselMetadata, Seq[VesselLocationRecord])],
 #      anchorages: SCollection[Anchorage],
 #      minVisitDuration: Duration
 #  ): SCollection[(VesselMetadata, immutable.Seq[AnchorageVisit])] = {
 #    val si = anchorages.asListSideInput
 #    val anchoragePointIdToAnchorageCache = ValueCache[Map[String, Anchorage]]()
 #    val anchorageLookupCache = ValueCache[AdjacencyLookup[AnchoragePoint]]()

 #    locationEvents
 #      .withSideInputs(si)
 #      .map {
 #        case ((metadata, locations), ctx) => {
 #          val anchoragePointIdToAnchorage = anchoragePointIdToAnchorageCache.get { () =>
 #            ctx(si).flatMap { ag =>
 #              ag.anchoragePoints.map { a =>
 #                (a.id, ag)
 #              }
 #            }.toMap
 #          }

 #          val lookup = anchorageLookupCache.get { () =>
 #            AdjacencyLookup(ctx(si).flatMap(_.anchoragePoints),
 #                            (anchorage: AnchoragePoint) => anchorage.meanLocation,
 #                            AnchorageParameters.anchorageVisitDistanceThreshold,
 #                            AnchorageParameters.anchoragesS2Scale)
 #          }

 #          (metadata,
 #           locations
 #             .map((location) => {
 #               val anchoragePoints = lookup.nearby(location.location)
 #               if (anchoragePoints.length > 0) {
 #                 Some(
 #                   AnchorageVisit(anchoragePointIdToAnchorage(anchoragePoints.head._2.id),
 #                                  location.timestamp,
 #                                  location.timestamp))
 #               } else {
 #                 None
 #               }
 #             })
 #             .foldLeft(Vector[Option[AnchorageVisit]]())((res, visit) => {
 #               if (res.length == 0) {
 #                 res :+ visit
 #               } else {
 #                 (visit, res.last) match {
 #                   case (None, None) => res
 #                   case (None, Some(last)) => res :+ None
 #                   case (Some(visit), None) => res.init :+ Some(visit)
 #                   case (Some(visit), Some(last)) =>
 #                     res.init ++ last.extend(visit).map(visit => Some(visit))
 #                 }
 #               }
 #             })
 #             .filter(_.nonEmpty)
 #             .map(_.head)
 #             .filter(_.duration.isLongerThan(minVisitDuration))
 #             .toSeq)
 #        }
 #      }
 #      .toSCollection
 #  }



def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline.



    python -m anchorages \
        --project world-fishing-827 \
        --job_name test-anchorages \
        --runner DataflowRunner \
        --input-pattern gs://p_p429_resampling_3/data-production/classify-pipeline/classify/*-*-*/*-of-* \
        --staging_location gs://world-fishing-827/scratch/timh/output/staging \
        --temp_location gs://world-fishing-827/scratch/timh/temp \
        --requirements_file requirements.txt \
        --max_num_workers 100

    python -m anchorages \
        --input-pattern gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-01-01/001-of-* \
        --max_num_workers 10


    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-patterns',
                                        # default='gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2012-*-*/*-of-*,'
                                        #         'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2013-*-*/*-of-*,'
                                        #         'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2014-*-*/*-of-*,'
                                        #         'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2015-*-*/*-of-*,'
                                        #         'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-*-*/*-of-*,'
                                        #         'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2017-*-*/*-of-*',
                                        default=
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-01-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-02-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-03-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-04-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-05-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-06-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-07-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-08-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-09-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-10-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-11-*/*-of-*,'
                                                'gs://p_p429_resampling_3/data-production/classify-pipeline/classify/2016-12-*/*-of-*',
                                            help='Input file to patterns (comma separated) to process (glob)')
    parser.add_argument('--output',
                                            dest='output',
                                            default='gs://world-fishing-827/scratch/timh/output/test_anchorages',
                                            help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    # TODO: Should go in config file or arguments >>>
    min_required_positions = 200 
    stationary_period_min_duration = datetime.timedelta(hours=48)
    stationary_period_max_distance = 0.5 # km
    min_unique_vessels_for_anchorage = 20
    blacklisted_mmsis = [0, 12345]
    # ^^^
    input_patterns = [x.strip() for x in known_args.input_patterns.split(',')]

    ais_input_data_streams = [(p | 'read_{}'.format(i) >> ReadFromText(x)) for (i, x) in  enumerate(input_patterns)]

    ais_input_data = ais_input_data_streams | beam.Flatten()

    location_records = read_json_records(ais_input_data, blacklisted_mmsis)

    grouped_records = location_records | "GroupByMmsi" >> beam.GroupByKey()

    deduped_records = filter_duplicate_timestamps(grouped_records, min_required_positions)

    processed = filter_and_process_vessel_records(deduped_records, stationary_period_min_duration, stationary_period_max_distance)

    anchorage_points = (find_anchorage_point_cells(processed, min_unique_vessels_for_anchorage) |
                        beam.Filter(lambda x: not inland_mask[x.mean_location]))


    anchorages = (GroupAll(anchorage_points, "GroupAllAnchorages")
        | "MergeAdjacentPoints" >> beam.FlatMap(merge_adjacent_anchorage_points))


    (anchorage_points 
        | beam.Map(lambda x: json.dumps(anchorage_point_to_json(x)))
        | 'writeAnchorages' >> WriteToText(known_args.output + '_anchorages', file_name_suffix='.json')
    )

    (anchorages 
        | beam.Map(Anchorages.to_json)
        | 'writeAnchoragePoints' >> WriteToText(known_args.output + '_anchorage_points', file_name_suffix='.json')
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
