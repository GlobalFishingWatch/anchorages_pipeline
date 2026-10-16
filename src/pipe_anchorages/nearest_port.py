import csv
import logging
import threading
from collections import namedtuple
from .distance import distance, inf

Port = namedtuple("Port", ["iso3", "label", "sublabel", "lat", "lon"])


class PortFinder:

    def __init__(self, path):
        self.ports = []
        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    self.ports.append(
                        Port(
                            iso3=row["iso3"],
                            label=row["label"],
                            sublabel=row["sublabel"],
                            lat=float(row["latitude"]),
                            lon=float(row["longitude"]),
                        )
                    )
                except ValueError:
                    logging.fatal("Could not parse row: '{}'".format(row))
                    raise

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
_lock = threading.Lock()


def get_port_finder(path):
    # Beam runs several worker threads per process: they wait for the first to load the file.
    with _lock:
        if path not in _cache:
            _cache[path] = PortFinder(path)
        return _cache[path]
