# from skimage import io
# import numpy as np
# imarray = io.imread("dist2coast_1deg_ocean_negative-land_v3.tiff")
# np.save("inland.npy", imarray == -10)
# sparsify("../inland.npy", "sparse_inland.pickle")

from __future__ import print_function, division
import pickle
import time
import bisect
import array


class Mask(object):

    @staticmethod
    def _load_mask(path, threshold, invert):
        import rasterio

        with rasterio.open(path) as src:
            [img] = src.read()
            height, width = img.shape
        [first_x, dx, shear_0, first_y, shear_1, dy] = src.transform
        assert shear_0 == shear_1 == 0, "shearing is not supported"
        assert dx > 0 and dy < 0, "only normal image orientation supported"
        last_x = first_x + dx * width
        last_y = first_y + dy * height

        min_x = min(first_x, last_x)
        max_x = max(first_x, last_x)
        min_y = min(first_y, last_y)
        max_y = max(first_y, last_y)

        mask = img > threshold
        if invert:
            mask = ~mask

        return (min_x, max_x, min_y, max_y), mask

    @classmethod
    def sparsify(cls, inpath, outpath, threshold=0.5, invert=False):
        import numpy as np

        (min_lon, max_lon, min_lat, max_lat), dense = cls._load_mask(inpath, threshold, invert)
        sparse = []
        indices = np.arange(dense.shape[1])
        assert dense.shape[1] <= 65535
        for i, row in enumerate(dense):
            # Ensure boolean and copy
            mask = row != 0
            # First element of mask is just row[0],
            # subsequent elements are true if that element
            # of row differs from the element previous.
            mask[1:] ^= row[:-1]
            # For each true element of mask store an index
            # as an unsigned short.
            sparse.append(array.array("H", indices[mask]))
        mask_info = {
            "min_lon": min_lon,
            "max_lon": max_lon,
            "min_lat": min_lat,
            "max_lat": max_lat,
            "n_lat": dense.shape[0],
            "n_lon": dense.shape[1],
            "data": tuple(sparse),
        }
        with open(outpath, "wb") as f:
            pickle.dump(mask_info, f)


class SimpleMask(Mask):

    def __init__(self, path, threshold, invert):
        import numpy as np

        bounds, self.mask = self._load_mask(path, threshold, invert)
        self.MIN_LON, self.MAX_LON, self.MIN_LAT, self.MAX_LAT = bounds
        self.nlat, self.nlon = self.mask.shape
        self.dlat = (self.MAX_LAT - self.MIN_LAT) / self.nlat
        self.dlon = (self.MAX_LON - self.MIN_LON) / self.nlon

    def query(self, loc):
        lat, lon = loc
        i = int((self.MAX_LAT - lat) // self.dlat)
        j = int((lon - self.MIN_LON) // self.dlon)
        return self.mask[i, j]


class SparseMask(Mask):

    def __init__(self, path):
        with open(path) as f:
            mask_info = pickle.load(f)
        self.mask_data = mask_info["data"]
        self.MAX_LAT = mask_info["max_lat"]
        self.MIN_LAT = mask_info["min_lat"]
        self.MAX_LON = mask_info["max_lon"]
        self.MIN_LON = mask_info["min_lon"]
        self._dlat = (self.MAX_LAT - self.MIN_LAT) / mask_info["n_lat"]
        self._dlon = (self.MAX_LON - self.MIN_LON) / mask_info["n_lon"]

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


class SparseInlandMask(SparseMask):

    def __init__(self):
        CompactMask.__init__(self, "sparse_inland.pickle")


def test(sparse_path, dense_path, threshold=0.5, invert=False):
    import numpy as np

    dense_mask = SimpleMask(dense_path, threshold, invert)
    sparse_mask = SparseMask(sparse_path)

    n = 100000
    results = []
    latlon = []
    for i in range(n):
        lat = np.random.uniform(dense_mask.MIN_LAT, dense_mask.MAX_LAT)
        lon = np.random.uniform(dense_mask.MIN_LON, dense_mask.MAX_LON)
        dense_result = dense_mask.query((lat, lon))
        sparse_result = sparse_mask.query((lat, lon))
        assert dense_result == sparse_result, (lat, lon, dense_result, sparse_result, i)
        results.append(dense_result)
        latlon.append((lat, lon))

    print("Average amount of land (by degrees)", np.mean(results))

    t0 = time.clock()
    for lat, lon in latlon:
        dense_result = dense_mask.query((lat, lon))
    d1 = time.clock() - t0
    print("Dense", d1, "seconds")

    t0 = time.clock()
    for lat, lon in latlon:
        dense_result = sparse_mask.query((lat, lon))
    d2 = time.clock() - t0
    print("Sparse", d2, "seconds")
    print("Time Ratio", d2 / d1)


if __name__ == "__main__":
    test("sparse_inland.pickle", "../inland.npy", 0.5)
