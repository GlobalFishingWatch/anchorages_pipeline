from __future__ import print_function, division
import csv
import os
import math
from collections import namedtuple
from .distance import distance, EARTH_RADIUS

Port = namedtuple("Port", ["name", "country", "lat", "lon"])

this_dir = os.path.dirname(__file__)


BUFFER_KM = 64 + 1
inf = float('inf')
assert math.isinf(inf)

class PortFinder(object):

    def __init__(self, anchorage_path="ports.csv"):
        self.ports_near = {}
        self.ports = []
        with open(os.path.join(this_dir, anchorage_path)) as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.ports.append(Port(name=row['port_name'],
                                       country=row['country'],
                                       lat=float(row['latitude']),
                                       lon=float(row['longitude'])))

    def __call__(self, loc):
        return self.find_nearest_port_and_distance(loc)[0]


    def find_nearest_port_and_distance(self, loc):
        min_p = None
        min_dist = inf
        for p in self.ports:
            dist = distance(p, loc)
            if dist > min_dist:
                continue
            min_p = p
            min_dist = dist
        return min_p, min_dist


    def is_within(self, range, location_record, s2id=None):
        loc = location_record.location
        if s2id is None:
            s2id = location_record.s2id
        if s2id not in self.ports_near:
            ports = []
            for p in self.ports:
                if distance(p, loc) <= BUFFER_KM:
                    ports.append(p)
            self.ports_near[s2id] = ports
        candidates = sorted([(distance(p, loc), p) for p in self.ports_near[s2id]])
        if candidates:
            dist, port = candidates[0]
            if dist <= range:
                return port
        return None


    def is_within_dist(self, range, location_record, s2id=None):
        loc = location_record.location
        if s2id is None:
            s2id = location_record.s2id
        if s2id not in self.ports_near:
            ports = []
            for p in self.ports:
                if distance(p, loc) <= BUFFER_KM:
                    ports.append(p)
            self.ports_near[s2id] = ports
        candidates = sorted([(distance(p, loc), p) for p in self.ports_near[s2id]])
        if candidates:
            dist, port = candidates[0]
            if dist <= range:
                return port, dist
        return None, inf


port_finder = PortFinder()


Anchorage = namedtuple("Anchorage", ["name", "country", "lat", "lon", "anchorage_point"])


class AnchorageFinder(PortFinder):

    def __init__(self, anchorages):
        self.ports_near = {}
        self.ports = [Anchorage(name=ap.port_name[0],
                                country=ap.port_name[1],
                                lat=ap.mean_location.lat,
                                lon=ap.mean_location.lon,
                                anchorage_point=ap) for ap in anchorages]

