# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # buffered `World Countries` polygons unioned & dissolved for anchorages labelling
#
# 2019-08-06
#
# Brian Wong
#
# _*updated 2020-03-27_
#
# Notebook used to produce a global land polygon with a 450m buffer. The output data was used for labelling anchorage's (at `world-fishing-827.anchorages.named_anchorages_v20190827`) boolean `dock` status in `world-countries-450m-anchorage-labeler-20191005.ipynb`. Following steps higlight include steps before this notebook.
#
# - Accessed orignial land polygons dataset here: https://www.arcgis.com/home/item.html?id=d974d9c6bc924ae0a2ffea0a46d71e3d
# - Applied 450m buffer applied in ArcGIS (QGIS and notebook tools were failing)
# - Results in 657 overlapping polygons
# - Here, we overlay the 657 polygons and dissolve all boundaries
# - Output file `world_countries_450m_buffer_cascaded_union.shp` used in next notebook to label anchorages as dock or anchored
# - Helpful links:
#     - https://stackoverflow.com/questions/40385782/make-a-union-of-polygons-in-geopandas-or-shapely-into-a-single-geometry
#     - https://shapely.readthedocs.io/en/stable/manual.html#cascading-unions

import geopandas as gpd

shp = gpd.read_file('world_countries_450m_buffer/world_countries_450m_buffer.shp')

print(shp)

shp.geometry

# +
# dumping all geometries to single list for cascaded_union

polygons = list(shp.geometry)
# -

len(polygons)

from shapely.ops import cascaded_union

output = gpd.GeoSeries(cascaded_union(polys))

output.to_file('world_countries_450m_buffer_cascaded_union.shp')

# ## check results

polygon = gpd.read_file('world_countries_450m_buffer_cascaded_union/world_countries_450m_buffer_cascaded_union.shp')

# make sure it's 1 giant polygon vs the 657 from before
polygon

len(polygon)

from matplotlib import pyplot as plt

f, ax = plt.subplots(1, figsize=(32, 32))
ax = polygon.plot(axes=ax)
plt.show()

#india bangladesh border
f, ax = plt.subplots(1, figsize=(12, 12))
minx, miny, maxx, maxy = 87.9403,21.4378,90.0404,22.5891
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax = polygon.plot(axes=ax)
plt.show()

# singapore
f, ax = plt.subplots(1, figsize=(12, 12))
minx, miny, maxx, maxy = 102.6434,0.4462,104.9221,1.6701
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax = polygon.plot(axes=ax)
plt.show()

# hong kong
f, ax = plt.subplots(1, figsize=(12, 12))
minx, miny, maxx, maxy = 113.5231,21.7994,114.5264,22.6356
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax = polygon.plot(axes=ax)
plt.show()

# new orleans
f, ax = plt.subplots(1, figsize=(12, 12))
minx, miny, maxx, maxy = -90.3162,28.8901,-88.7123,29.9672
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax = polygon.plot(axes=ax)
plt.show()


