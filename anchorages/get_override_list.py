from . import common as cmn
import csv

_cached = None

def get_override_list(path):
    global _cached
    if _cached is None:
        _cached = []
        with open(path) as csvfile:
            for x in  csv.DictReader(csvfile):
                x['latLon'] = cmn.LatLon(float(x['latitude']), float(x['longitude']))
                x['s2id'] = x['latLon'].S2CellId(scale=cmn.ANCHORAGES_S2_SCALE).to_token()
                _cached.append(x) 
    return _cached