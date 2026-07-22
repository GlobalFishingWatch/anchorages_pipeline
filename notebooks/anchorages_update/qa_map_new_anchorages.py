#!/usr/bin/env python
# coding: utf-8

# In[1]:


from amanda_notebook_bq_helper import *
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import sys
from amanda_map_helper import *
from amanda_anchorage_helper import *

FIG_FLDR = './figures'


# In[2]:


from s2sphere import CellId, Cell, LatLng
import folium
from folium.features import GeoJson
import pandas as pd


# In[3]:


named_anchorages_table = 'world-fishing-827.scratch_amanda_ttl_120.named_anchorages_v20260302'


# ### check for duplicates

# In[4]:


q = f'''
SELECT *
FROM `{named_anchorages_table}`
QUALIFY COUNT(*) OVER(PARTITION BY s2id) > 1
'''
get_bq_df(q)


# ### Make sure s2 cells are the same as what we’d get if we ran Nate’s additional command

# In[ ]:


# Query that applies Nate’s updates then checks against the new table so we can check that the results would have the same s2 cells
q = f'''
WITH 

good_labels AS (SELECT
point_id As s2id,
port_label AS label,
point_label AS sublabel,
iso3 AS iso3
FROM
`world-fishing-827.scratch_nate.final_gfw_ports_with_ids_v20220421`
),

updated_labels AS (SELECT
DISTINCT *
FROM
good_labels
LEFT JOIN
(SELECT
AVG(lat) AS lat,
AVG(lon) AS lon,
total_visits,
drift_radius,
top_destination,
unique_stationary_ssvid,
unique_stationary_fishing_ssvid,
unique_active_ssvid,
unique_total_ssvid,
active_ssvid_days,
stationary_ssvid_days,
stationary_fishing_ssvid_days,
s2id,
distance_from_shore_m,
label_source,
dock
FROM
`{named_anchorages_table}`
GROUP BY 3,4,5,6,7,8,9,10,11,12,13,14,15,16
)
USING (s2id)
),
missing_antarctic_anchorages AS (
SELECT 
s2id,
label,
sublabel,
iso3,
lat,
lon,
total_visits,
drift_radius,
unique_stationary_ssvid,
unique_stationary_fishing_ssvid,
unique_active_ssvid,
unique_total_ssvid,
active_ssvid_days,
stationary_ssvid_days,
stationary_fishing_ssvid_days,
distance_from_shore_m,
top_destination,
label_source,
dock
 FROM `{named_anchorages_table}`
WHERE s2id NOT IN ('140edc01','5aaaaaab','50000001') AND
s2id NOT IN (SELECT s2id FROM updated_labels)
),

final AS (SELECT 
s2id,
label,
sublabel,
iso3,
lat,
lon,
total_visits,
drift_radius,
unique_stationary_ssvid,
unique_stationary_fishing_ssvid,
unique_active_ssvid,
unique_total_ssvid,
active_ssvid_days,
stationary_ssvid_days,
stationary_fishing_ssvid_days,
distance_from_shore_m,
top_destination,
label_source,
dock
FROM updated_labels

UNION ALL
SELECT
*
FROM
missing_antarctic_anchorages
),

gfw_ports_database AS (
    SELECT
    *
    FROM
    `world-fishing-827.proj_pew_ports.gfw_ports_database_v20230424`
),

merge_abidjan AS (
    SELECT 
    * 
    FROM 
    final 
    LEFT JOIN
    (SELECT
    point_id AS s2id,
    cluster_label AS sublabel2
    FROM
    gfw_ports_database 
    WHERE 
    port_label = 'ABIDJAN'
    )
    USING (s2id)
    ),

update_abidjan AS (
SELECT 
s2id,
label,
IF(sublabel2 IS NULL, sublabel, sublabel2) AS sublabel,
iso3,
lat,
lon,
total_visits,
drift_radius,
unique_stationary_ssvid,
unique_stationary_fishing_ssvid,
unique_active_ssvid,
unique_total_ssvid,
active_ssvid_days,
stationary_ssvid_days,
stationary_fishing_ssvid_days,
distance_from_shore_m,
top_destination,
IF(sublabel2 IS NOT NULL, 'tmt',label_source) AS label_source,
dock
FROM
merge_abidjan
),

merge_senegal AS (
    SELECT 
    * 
    FROM 
    update_abidjan 
    LEFT JOIN
    (SELECT
    s2id,
    sublabel AS sublabel2
    FROM
    `world-fishing-827.scratch_nate.sen_tmt_anchorage_labels` 
    )
    USING (s2id)
    ),

update_senegal AS (
SELECT 
s2id,
label,
IF(sublabel2 IS NULL, sublabel, sublabel2) AS sublabel,
iso3,
lat,
lon,
total_visits,
drift_radius,
unique_stationary_ssvid,
unique_stationary_fishing_ssvid,
unique_active_ssvid,
unique_total_ssvid,
active_ssvid_days,
stationary_ssvid_days,
stationary_fishing_ssvid_days,
distance_from_shore_m,
top_destination,
IF(sublabel2 IS NOT NULL, 'tmt',label_source) AS label_source,
dock
FROM
merge_senegal
),


merge_guinea AS (
    SELECT 
    * 
    FROM 
    update_senegal 
    LEFT JOIN
    (SELECT
    s2id,
    sublabel AS sublabel2
    FROM
    `world-fishing-827.scratch_nate.gin_tmt_anchorage_labels`
    )
    USING (s2id)
    ),

update_guinea AS (
SELECT 
s2id,
label,
IF(sublabel2 IS NULL, sublabel, sublabel2) AS sublabel,
iso3,
lat,
lon,
total_visits,
drift_radius,
unique_stationary_ssvid,
unique_stationary_fishing_ssvid,
unique_active_ssvid,
unique_total_ssvid,
active_ssvid_days,
stationary_ssvid_days,
stationary_fishing_ssvid_days,
distance_from_shore_m,
top_destination,
IF(sublabel2 IS NOT NULL, 'tmt',label_source) AS label_source,
dock
FROM
merge_guinea
),


merge_kenya AS (
    SELECT 
    * 
    FROM 
    update_guinea 
    LEFT JOIN
    (SELECT
    s2id,
    sublabel AS sublabel2
    FROM
    `world-fishing-827.scratch_nate.gin_tmt_anchorage_labels`
    )
    USING (s2id)
    ),

update_kenya AS (
SELECT 
s2id,
label,
IF(sublabel2 IS NULL, sublabel, sublabel2) AS sublabel,
iso3,
lat,
lon,
total_visits,
drift_radius,
unique_stationary_ssvid,
unique_stationary_fishing_ssvid,
unique_active_ssvid,
unique_total_ssvid,
active_ssvid_days,
stationary_ssvid_days,
stationary_fishing_ssvid_days,
distance_from_shore_m,
top_destination,
IF(sublabel2 IS NOT NULL, 'tmt',label_source) AS label_source,
dock
FROM
merge_kenya
),


merge_ghana AS (
    SELECT 
    * 
    FROM 
    update_guinea 
    LEFT JOIN
    (SELECT
    s2id,
    sublabel AS sublabel2
    FROM
    `world-fishing-827.scratch_nate.gha_tmt_anchorage_labels`
    )
    USING (s2id)
    ),

update_ghana AS (
SELECT 
s2id,
label,
IF(sublabel2 IS NULL, sublabel, sublabel2) AS sublabel,
iso3,
lat,
lon,
total_visits,
drift_radius,
unique_stationary_ssvid,
unique_stationary_fishing_ssvid,
unique_active_ssvid,
unique_total_ssvid,
active_ssvid_days,
stationary_ssvid_days,
stationary_fishing_ssvid_days,
distance_from_shore_m,
top_destination,
IF(sublabel2 IS NOT NULL, 'tmt',label_source) AS label_source,
dock
FROM
merge_ghana
),

postnate_0 AS ( --This is the output of Nate's additional query
SELECT 
s2id,
label,
REPLACE(sublabel, 'ANCHORAGE', 'OFFSHORE') AS sublabel,
iso3,
lat,
lon,
total_visits,
drift_radius,
unique_stationary_ssvid,
unique_stationary_fishing_ssvid,
unique_active_ssvid,
unique_total_ssvid,
active_ssvid_days,
stationary_ssvid_days,
stationary_fishing_ssvid_days,
distance_from_shore_m,
top_destination,
label_source,
dock 
FROM update_ghana
),

postnate AS (
  SELECT DISTINCT s2id
  FROM postnate_0
),
prenate AS (
  SELECT DISTINCT s2id
  FROM `{named_anchorages_table}`
)
SELECT
  COALESCE(p.s2id, pre.s2id) AS s2id,
  CASE
    WHEN pre.s2id IS NULL THEN 'postnate'
    WHEN p.s2id IS NULL THEN 'prenate'
  END AS which_table
FROM postnate p
FULL OUTER JOIN prenate pre USING (s2id)
WHERE p.s2id IS NULL OR pre.s2id IS NULL;
'''
get_bq_df(q)


# ### check counts of new/removed against existing named_anchorages table

# In[12]:


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
  mismatch_location,
  iso3,
  COUNT(*) AS row_count,
  label_source,
FROM
  compare
GROUP BY mismatch_location, iso3, label_source
ORDER BY row_count DESC

'''
df = get_bq_df(q)
df


# ### get all rows with s2ids that exist in only one of the new or existing named_anchorages table

# In[13]:


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
df = fresh_get_bq_df(q)
df['source'] = df['iso3']
df


# In[14]:


df.to_csv('./added_or_removed_s2ids_v20260302.csv',index=False)


# In[5]:


len(df[df['mismatch_location']=='Only in scratch_amanda'])


# In[6]:


df[df['mismatch_location']=='Only in global-fishing-watch']


# Get the lat/lon of the one deleted anchorage

# In[7]:


s2id_to_latlon('8efe7543')


# In[ ]:





# In[12]:


filtered_df = df[df['mismatch_location'] == 'Only in scratch_amanda']
counts = filtered_df.groupby('iso3').size().reset_index(name='count').sort_values('count', ascending=False).reset_index(drop=True)
counts


# In[13]:


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


# In[ ]:


m = map_s2_anchorages(df, show_labels=False, fit_bounds=True,legend_title='iso3',color_map=country_color_map)
m


# In[ ]:


map_file = f"{FIG_FLDR}/new_anchorages.html"
m.save(map_file)
print(f"Saved map to {map_file}")


# In[ ]:




