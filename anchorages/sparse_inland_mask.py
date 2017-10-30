import bisect
import os
import pickle


this_dir = os.path.dirname(__file__)

class SparseInlandMask(object):

    def __init__(self):
        with open(os.path.join(this_dir, "sparse_inland.pickle")) as f:
            mask_info = pickle.load(f)
        self.mask_data = mask_info['data']
        self.MAX_LAT = mask_info['max_lat']
        self.MIN_LAT = mask_info['min_lat']
        self.MAX_LON = mask_info['max_lon']
        self.MIN_LON = mask_info['min_lon']
        self._dlat = (self.MAX_LAT - self.MIN_LAT) / mask_info['n_lat']
        self._dlon = (self.MAX_LON - self.MIN_LON) / mask_info['n_lon']

    def is_inland(self, loc):
        lat, lon = loc
        i = (self.MAX_LAT - lat) // self._dlat
        j = (lon - self.MIN_LON) // self._dlon
        ndx = bisect.bisect_right(self.mask_data[int(i)], j)
        return ndx & 1

    def checked_query(self, loc):
        lat, lon = loc
        assert self.MIN_LAT <= lat < self.MAX_LAT
        assert self.MIN_LON <= lat < self.MAX_LON
        return not self.is_inland(loc)