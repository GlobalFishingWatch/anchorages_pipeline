from __future__ import print_function, division
import csv
import os
import math
from collections import namedtuple
from .distance import distance, EARTH_RADIUS, inf

Port = namedtuple("Port", ["iso3", "label", "sublabel", "lat", "lon"])


BUFFER_KM = 64 + 1

class PortFinder(object):

    def __init__(self, path):
        self.ports_near = {}
        self.ports = []
        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.ports.append(Port(iso3=row['iso3'],
                                       label=row['label'],
                                       sublabel=row['sublabel'],
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

   

_cache = {}

def get_port_finder(path):
    if path not in _cache:
        _cache[path] = PortFinder(path)
    return _cache[path]

