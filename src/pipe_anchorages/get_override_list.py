from . import common as cmn
import csv

_cache = {}


def get_override_list(path):
    if path not in _cache:
        rows = []
        with open(path) as csvfile:
            for x in csv.DictReader(csvfile):
                x["latLon"] = cmn.LatLon(float(x["latitude"]), float(x["longitude"]))
                x["s2id"] = x["latLon"].S2CellId(scale=cmn.ANCHORAGES_S2_SCALE).to_token()
                rows.append(x)
        _cache[path] = rows
    return _cache[path]
