# from skimage import io
# import numpy as np
# imarray = io.imread("dist2coast_1deg_ocean_negative-land_v3.tiff")
# np.save("inland.npy", imarray == -10)

from __future__ import print_function, division
import pickle
import time
import bisect
import array


class InlandMask(object):

    MIN_LON = -180.0044166
    MAX_LON = 180.0044166
    MIN_LAT = -90.0022083
    MAX_LAT = 90.0022083

    def __init__(self):
        import numpy as np
        self.mask = np.load("../inland.npy")
        self.nlat, self.nlon = self.mask.shape
        self.dlat = (self.MAX_LAT - self.MIN_LAT) / self.nlat
        self.dlon = (self.MAX_LON - self.MIN_LON) / self.nlon

    def query(self, loc):
        lat, lon = loc
        i = int((self.MAX_LAT - lat) // self.dlat)
        j = int((lon - self.MIN_LON) // self.dlon)
        return self.mask[i, j]



class SparseInlandMask(object):

    def __init__(self):
        with open("sparse_inland.pickle") as f:
            mask_info = pickle.load(f)
        self.mask_data = mask_info['data']
        self.MAX_LAT = mask_info['max_lat']
        self.MIN_LAT = mask_info['min_lat']
        self.MAX_LON = mask_info['max_lon']
        self.MIN_LON = mask_info['min_lon']
        self._dlat = (self.MAX_LAT - self.MIN_LAT) / mask_info['n_lat']
        self._dlon = (self.MAX_LON - self.MIN_LON) / mask_info['n_lon']

    def query(self, loc):
        lat, lon = loc
        i = (self.MAX_LAT - lat) // self._dlat
        j = (lon - self.MIN_LON) // self._dlon
        ndx = bisect.bisect_right(self.mask_data[int(i)], j)
        return ndx & 1

    def checked_query(self, loc):
        lat, lon = loc
        assert self.MIN_LAT <= lat < self.MAX_LAT
        assert self.MIN_LON <= lat < self.MAX_LON
        return self.query(loc)







def sparsify():
    import numpy as np
    dense = np.load("../inland.npy").astype(bool)
    sparse = []
    indices = np.arange(dense.shape[1])
    assert dense.shape[1] <= 65535
    for i, row in enumerate(dense):
        # Ensure boolean and copy
        mask = (row != 0) 
        # First element of mask is just row[0],
        # subsequent elements are true if that element
        # of row differs from the element previous.
        mask[1:] ^= row[:-1]
        # For each true element of mask store an index
        # as an unsigned short.
        sparse.append(array.array('H', indices[mask]))
    mask_info = {
        'min_lon': -180.0044166,
        'max_lon': 180.0044166,
        'min_lat': -90.0022083,
        'max_lat': 90.0022083,
        'n_lat': dense.shape[0],
        'n_lon': dense.shape[1],
        'data': tuple(sparse)
    }
    with open("sparse_inland.pickle", "wb") as f:
        pickle.dump(mask_info, f)


# sparsify()


def test():
    import numpy as np
    dense_mask = InlandMask()
    sparse_mask = SparseInlandMask()

    n = 100000
    results = []
    latlon = []
    for i in range(n):
        lat = np.random.uniform(dense_mask.MIN_LAT, dense_mask.MAX_LAT)
        lon = np.random.uniform(dense_mask.MIN_LON, dense_mask.MAX_LON)
        dense_result = dense_mask.query((lat, lon))
        sparse_result = sparse_mask.query((lat, lon))
        assert dense_result == sparse_result, (lat, lon, dense_result, sparse_result)
        results.append(dense_result)
        latlon.append((lat, lon))

    print("Average amount of land (by degrees)", np.mean(results))

    t0 = time.clock()
    for (lat, lon) in latlon:
        dense_result = dense_mask.query((lat, lon))
    d1 = time.clock() - t0
    print("Dense", d1)

    t0 = time.clock()
    for (lat, lon) in latlon:
        dense_result = sparse_mask.query((lat, lon))
    d2 = time.clock() - t0
    print("Sparse", d2)
    print("Ratio", d2 / d1)

if __name__ == "__main__":
    test()
