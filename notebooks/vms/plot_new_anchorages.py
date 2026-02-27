#!/usr/bin/env python
# coding: utf-8

# In[42]:


from amanda_notebook_bq_helper import *
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import sys
from amanda_map_helper import *
from amanda_anchorage_helper import *

FIG_FLDR = './figures'


# In[43]:


from s2sphere import CellId, Cell, LatLng
import folium
from folium.features import GeoJson
import pandas as pd


# In[44]:


named_anchorages_table = 'world-fishing-827.scratch_amanda_ttl_120.named_anchorages_v20260226'

q = f'''
WITH

newt AS (
  SELECT
s2id, lat, lon, label, sublabel, iso3, distance_from_shore_m, label_source, drift_radius, total_visits, unique_total_ssvid, unique_active_ssvid, active_ssvid_days, unique_stationary_ssvid, stationary_ssvid_days, stationary_fishing_ssvid_days, unique_stationary_fishing_ssvid, top_destination, dock
FROM
`{named_anchorages_table}`
),

compare AS (

SELECT 
  'Only in scratch_amanda' AS mismatch_location, 
  a.*
FROM newt AS a
WHERE a.s2id NOT IN (
  SELECT s2id FROM `global-fishing-watch.anchorages.named_anchorages` WHERE s2id IS NOT NULL
)

UNION ALL

SELECT 
  'Only in global-fishing-watch' AS mismatch_location, 
  b.*
FROM `global-fishing-watch.anchorages.named_anchorages` AS b
WHERE b.s2id NOT IN (
  SELECT s2id FROM newt WHERE s2id IS NOT NULL
)
)

SELECT
  *
FROM
  compare
'''
df = get_bq_df(q)
df['source'] = df['iso3']


# In[45]:


df


# In[48]:


np.unique(df['source'])
country_color_map = {
    'BRA': '#1f77b4',  # Muted Blue
    'CHL': '#ff7f0e',  # Safety Orange
    'CRI': '#2ca02c',  # Asparagus Green
    'ECU': '#d62728',  # Brick Red
    'IDN': '#9467bd',  # Muted Purple
    'KEN': '#8c564b',  # Chestnut Brown
    'MNE': '#e377c2',  # Raspberry Pink
    'NIC': '#7f7f7f',  # Middle Gray
    'PAN': '#bcbd22',  # Curry Yellow-Green
    'PHL': '#17becf',  # Blue-Teal
    'PNG': '#d900ff',  # Purple pink
    'SLV': '#ffbb78',  # Light Orange
    'VNM': "#034A2B",   # Light Green
    'PER': "#dbdb8d",    # "Golden khaki",
    'SAU': "#dbff8d"
}


# In[49]:


m = map_s2_anchorages(df, show_labels=False, fit_bounds=True,legend_title='iso3',color_map=country_color_map)
m


# In[51]:


map_file = f"{FIG_FLDR}/new_anchorages.html"
m.save(map_file)
print(f"Saved map to {map_file}")

