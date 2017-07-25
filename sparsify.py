# from skimage import io
# import numpy as np
# imarray = io.imread("dist2coast_1deg_ocean_negative-land_v3.tiff")
# np.save("inland.npy", imarray == -10)

from __future__ import print_function, division
import numpy as np
import pickle
import time


class InlandMask(object):

    MIN_LON = -180.0044166
    MAX_LON = 180.0044166
    MIN_LAT = -90.0022083
    MAX_LAT = 90.0022083

    def __init__(self):
        self.mask = np.load("inland.npy")
        self.nlat, self.nlon = self.mask.shape
        self.dlat = (self.MAX_LAT - self.MIN_LAT) / self.nlat
        self.dlon = (self.MAX_LON - self.MIN_LON) / self.nlon

    def __getitem__(self, loc):
        lat, lon = loc
        i = int((self.MAX_LAT - lat) // self.dlat)
        j = int((lon - self.MIN_LON) // self.dlon)
        return self.mask[i, j]


class SparseInlandMask(object):

    def __init__(self):
        with open("sparse_inland.pickle") as f:
            mask_info =pickle.load(f)
        self.mask_data = mask_info['data']
        self.nlat = mask_info['n_lat']
        self.nlon = mask_info['n_lon']
        self.MAX_LAT = mask_info['max_lat']
        self.MIN_LAT = mask_info['min_lat']
        self.MAX_LON = mask_info['max_lon']
        self.MIN_LON = mask_info['min_lon']
        self.dlat = (self.MAX_LAT - self.MIN_LAT) / self.nlat
        self.dlon = (self.MAX_LON - self.MIN_LON) / self.nlon

    def __getitem__(self, loc):
        lat, lon = loc
        i = int((self.MAX_LAT - lat) // self.dlat)
        j = int((lon - self.MIN_LON) // self.dlon)
        # base is the value at MIN_LON and indices are pixels where it flips.
        base, not_base, indices = self.mask_data[i]
        ndx = np.searchsorted(indices, j, side='right')
        # If we have an odd number of flips then reverse base
        return not_base if (ndx & 1) else base


def sparsify():
    dense = np.load("inland.npy").astype(bool)
    sparse = []
    indices = np.arange(1, dense.shape[1])
    for i, row in enumerate(dense):
        mask = row[1:] ^ row[:-1]
        sparse.append((bool(row[0]), not row[0], indices[mask]))
    mask_info = {
        'min_lon': -180.0044166,
        'max_lon': 180.0044166,
        'min_lat': -90.0022083,
        'max_lat': 90.0022083,
        'n_lat': dense.shape[0],
        'n_lon': dense.shape[1],
        'data': sparse
    }
    with open("sparse_inland.pickle", "wb") as f:
        pickle.dump(mask_info, f)


# sparsify()


dense_mask = InlandMask()
sparse_mask = SparseInlandMask()


n = 100000
results = []
latlon = []
for i in range(n):
    lat = np.random.uniform(dense_mask.MIN_LAT, dense_mask.MAX_LAT)
    lon = np.random.uniform(dense_mask.MIN_LON, dense_mask.MAX_LON)
    dense_result = dense_mask[(lat, lon)] 
    sparse_result = sparse_mask[(lat, lon)]
    assert dense_result == sparse_result, (lat, lon, dense_result, sparse_result)
    results.append(dense_result)
    latlon.append((lat, lon))

print("Average amount of land (by degrees)", np.mean(results))



t0 = time.clock()
for (lat, lon) in latlon:
    dense_result = dense_mask[(lat, lon)] 
print("Dense", time.clock() - t0)

t0 = time.clock()
for (lat, lon) in latlon:
    dense_result = sparse_mask[(lat, lon)] 
print("Sparse", time.clock() - t0)
