from __future__ import absolute_import, print_function, division

import s2sphere
import math
from collections import namedtuple, Counter

import apache_beam as beam

from . import common as cmn
from .distance import distance
from .port_name_filter import normalized_valid_names
from .sparse_inland_mask import SparseInlandMask


StationaryPeriod = namedtuple("StationaryPeriod", 
    ['location', 'start_time', 'duration', 'rms_drift_radius', 'destination'])

ActiveAndStationary = namedtuple("ActiveAndStationary", 
    ['active_records', 'stationary_periods'])


class FindAnchoragePoints(beam.PTransform):

    def __init__(self, min_duration, max_distance, min_unique_vessels, fishing_vessel_list):
        self.min_duration = min_duration
        self.max_distance = max_distance
        self.min_unique_vessels = min_unique_vessels
        self.fishing_vessel_list = fishing_vessel_list
        self.fishing_vessel_set = None
        self.inland_mask = SparseInlandMask()

    def split_on_movement(self, item):
        # extract long stationary periods from the record. Stationary periods are returned 
        # separately: anything over the threshold time will be reduced to just the start 
        # and end points of the period. The remaining points will summarized and returned
        # as a stationary period
        ssvid, records = item

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

        return (ssvid, ActiveAndStationary(active_records=active_records, 
                                          stationary_periods=stationary_periods))

    def extract_stationary(self, item):
        ssvid, combined = item
        return [(sp.location.S2CellId(cmn.ANCHORAGES_S2_SCALE).to_token(), (ssvid, sp)) 
                        for sp in combined.stationary_periods] 

    def extract_active(self, item):
        ssvid, combined = item
        return [(ar.location.S2CellId(cmn.ANCHORAGES_S2_SCALE).to_token(), (ssvid, ar)) 
                        for ar in combined.active_records] 

    def create_anchorage_pts(self, item, fishing_vessel_list):
        if self.fishing_vessel_set is None:
            self.fishing_vessel_set = set(fishing_vessel_list)
        value = AnchoragePoint.from_cell_visits(item, self.fishing_vessel_set)
        return [] if (value is None) else [value]

    def has_enough_vessels(self, item):
        return len(item.vessels) >= self.min_unique_vessels

    # def not_inland(self, item):
    #     return not self.inland_mask.is_inland(item.mean_location)

    def expand(self, ais_source):
        combined = ais_source | beam.Map(self.split_on_movement)
        stationary = combined | beam.FlatMap(self.extract_stationary)
        active =     combined | beam.FlatMap(self.extract_active)
        return ((stationary, active)
            | beam.CoGroupByKey()
            | beam.FlatMap(self.create_anchorage_pts, self.fishing_vessel_list)
            | beam.Filter(self.has_enough_vessels)
            # | beam.Filter(self.not_inland)
            )


class AnchoragePoint(namedtuple("AnchoragePoint", ['mean_location',
                                                  'total_visits',
                                                  'vessels',
                                                  'fishing_vessels',
                                                  'rms_drift_radius',
                                                  'top_destination',
                                                  's2id',
                                                  'neighbor_s2ids',
                                                  'active_ssvids',
                                                  'total_ssvids',
                                                  'stationary_ssvid_days',
                                                  'stationary_fishing_ssvid_days',
                                                  'active_ssvid_days',
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
        active_ssvids = set(md for (md, loc) in active_points)
        active_ssvid_count = len(active_ssvids)
        active_days = len(set([(md, loc.timestamp.date()) for (md, loc) in active_points]))
        stationary_days = 0
        stationary_fishing_days = 0

        for (ssvid, sp) in stationary_periods:
            n += 1
            total_lat += sp.location.lat
            total_lon += sp.location.lon
            vessels.add(ssvid)
            stationary_days += sp.duration.total_seconds() / (24.0 * 60.0 * 60.0)
            if ssvid in fishing_vessel_set:
                fishing_vessels.add(ssvid)
                stationary_fishing_days += sp.duration.total_seconds() / (24.0 * 60.0 * 60.0)
            total_squared_drift_radius += sp.rms_drift_radius ** 2
        all_destinations = normalized_valid_names(sp.destination for (md, sp) in stationary_periods)

        total_ssvid_count = len(vessels | active_ssvids)

        if n:
            neighbor_s2ids = tuple(s2sphere.CellId.from_token(s2id).get_all_neighbors(cmn.ANCHORAGES_S2_SCALE))
            loc = cmn.LatLon(total_lat / n, total_lon / n)

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
                        active_ssvids = active_ssvid_count,
                        total_ssvids = total_ssvid_count,
                        stationary_ssvid_days = stationary_days,
                        stationary_fishing_ssvid_days = stationary_fishing_days,
                        active_ssvid_days = active_days,
                        )
        else:
            return None






