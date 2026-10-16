from . import common as cmn
import csv
import threading

_cache = {}
_lock = threading.Lock()


def get_override_list(path):
    # Beam runs several worker threads per process: they wait for the first to load the file.
    with _lock:
        if path not in _cache:
            rows = []
            with open(path) as csvfile:
                for x in csv.DictReader(csvfile):
                    x["latLon"] = cmn.LatLon(float(x["latitude"]), float(x["longitude"]))
                    x["s2id"] = x["latLon"].S2CellId(scale=cmn.ANCHORAGES_S2_SCALE).to_token()
                    rows.append(x)
            _cache[path] = rows
        return _cache[path]
