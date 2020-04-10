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

# # Label anchorages dock status using buffered World Countries polygon
#
# 2019-08-13
#
# Brian Wong
#
# _*updated 2020-03-27_
#
# This notebook adds a `dock` label to anchorages (`gfw_research.named_anchorages_v20190621`) as `true` or `false` based on if it falls within the 450m buffered land polygon dataset or not. Uploaded to BQ after visual inspection here.
#
# - Requires `world_countries_450m_buffer_cascaded_union.shp` produced in `world-countries-450m-buffer-cascaded-union.ipynb` to run notebook
# - the .shp file is too large to store in GitHub so is located in GCS at `gs://brian-scratch/world_countries_450m_buffer_cascaded_union.zip`
# - Helpful link about geopandas `sjoin` function used: https://gis.stackexchange.com/questions/282681/filter-a-geopandas-dataframe-for-points-within-a-specific-country
# - Current output table set to `scratch_brian.anchorages_dock_label`
#
#
# # Output usage
#
# use following to query `anchorages` table with new boolean `dock` labels:
#
# ``` sql
# with 
#     anchorages as (select * from `world-fishing-827.anchorages.named_anchorages_v20190621`),
#     dock_labels as (select s2id , dock from `world-fishing-827.scratch_brian.anchorages_dock_label`),
#     labeled as(select * from anchorages left join dock_labels using (s2id))
# select * from labeled
# ```

import pandas as pd
import numpy as np
import geopandas as gpd
from geopandas.tools import sjoin

# +
# get named anchorages

q = '''
SELECT lon, lat, s2id FROM `world-fishing-827.anchorages.named_anchorages_v20190827`
'''

df = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
# convert to geodataframe

gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
# -

gdf.head()

len(gdf)

# read big polygon 
shp = gpd.read_file('world_countries_450m_buffer_cascaded_union/world_countries_450m_buffer_cascaded_union.shp')

shp

# +
# sjoin results: if 'id' = 0.0 it's within, otherwise NaNs are not

pointInPolys = sjoin(gdf, shp, how='left')
pointInPolys.head()
# -

# back to df for query
df = pd.DataFrame(pointInPolys)

# query df for TRUE and FALSE
pts_FALSE = df.query('id != id')
pts_TRUE = df.query('id == 0.0')

# convert back to geodataframe
gdf_TRUE = gpd.GeoDataFrame(pts_TRUE)
gdf_FALSE = gpd.GeoDataFrame(pts_FALSE)

gdf_TRUE.head()

gdf_FALSE.head()

from matplotlib import pyplot as plt

# singapore
f, ax = plt.subplots(1, figsize=(24, 24))
minx, miny, maxx, maxy = 103.4563,0.8895,104.3177,1.5351
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax = shp.plot(axes=ax)
ax = gdf_TRUE.plot(axes=ax,color='orange',markersize=9)
ax = gdf_FALSE.plot(axes=ax,color='green',markersize=9)
plt.show()

# mississippi
f, ax = plt.subplots(1, figsize=(24, 24))
minx, miny, maxx, maxy = -91.9,28.68,-88.77,30.77
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax = shp.plot(axes=ax)
ax = gdf_TRUE.plot(axes=ax,color='orange',markersize=3)
ax = gdf_FALSE.plot(axes=ax,color='green',markersize=3)
plt.show()

# shanghai up river
f, ax = plt.subplots(1, figsize=(24, 24))
minx, miny, maxx, maxy = 117.04,29.64,123.34,32.85
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax = shp.plot(axes=ax)
ax = gdf_TRUE.plot(axes=ax,color='orange',markersize=1)
ax = gdf_FALSE.plot(axes=ax,color='green',markersize=1)
plt.show()

# hong kong
f, ax = plt.subplots(1, figsize=(24, 24))
minx, miny, maxx, maxy = 113.5231,21.7994,114.5264,22.6356
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax = shp.plot(axes=ax)
ax = gdf_TRUE.plot(axes=ax,color='orange',markersize=9)
ax = gdf_FALSE.plot(axes=ax,color='green',markersize=9)
plt.show()

# abidjan
f, ax = plt.subplots(1, figsize=(24, 24))
minx, miny, maxx, maxy = -4.121455,5.173169,-3.940917,5.366072
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax = shp.plot(axes=ax)
ax = gdf_TRUE.plot(axes=ax,color='orange',markersize=22)
ax = gdf_FALSE.plot(axes=ax,color='green',markersize=22)
plt.show()

# shanghai
f, ax = plt.subplots(1, figsize=(24, 24))
minx, miny, maxx, maxy = 119.6637,29.2433,123.2773,32.1047
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax = shp.plot(axes=ax)
ax = gdf_TRUE.plot(axes=ax,color='orange',markersize=4)
ax = gdf_FALSE.plot(axes=ax,color='green',markersize=4)
plt.show()

# tokyo
f, ax = plt.subplots(1, figsize=(24, 24))
minx, miny, maxx, maxy = 139.5966,35.2597,140.1374,35.7075
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax = shp.plot(axes=ax)
ax = gdf_TRUE.plot(axes=ax,color='orange',markersize=9)
ax = gdf_FALSE.plot(axes=ax,color='green',markersize=9)
plt.show()

# tianjian
f, ax = plt.subplots(1, figsize=(24, 24))
minx, miny, maxx, maxy = 117.3104,37.9443,119.3134,39.3913
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)
ax = shp.plot(axes=ax)
ax = gdf_TRUE.plot(axes=ax,color='orange',markersize=5)
ax = gdf_FALSE.plot(axes=ax,color='green',markersize=5)
plt.show()

# ## bq upload
#
# Looks good so reconstruct df for bq upload with boolean `dock` labels

df

df['dock'] = (df['id']==0.0)

df

df = df.drop(columns=['index_right','id'])

df.head()

len(df.query('dock == False'))

len(df.query('dock == True'))

print(len(df.query('dock == True'))/len(df))

df.to_gbq('scratch_brian.anchorages_dock_label', project_id='world-fishing-827', if_exists='replace')


