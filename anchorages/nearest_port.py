from __future__ import print_function, division
import csv
import os
import math
from collections import namedtuple
from .distance import distance, EARTH_RADIUS, inf

Port = namedtuple("Port", ["name", "country", "lat", "lon"])

this_dir = os.path.dirname(__file__)

BUFFER_KM = 64 + 1

class PortFinder(object):

    def __init__(self, path):
        self.ports_near = {}
        self.ports = []
        with open(os.path.join(this_dir, path)) as f:
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

   

wpi_finder = PortFinder('WPI_ports.csv')
geo_finder = PortFinder('geonames_1000.csv')
