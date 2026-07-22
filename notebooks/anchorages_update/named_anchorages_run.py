#!/usr/bin/env python
# coding: utf-8

# see notes here https://www.notion.so/globalfishingwatch/Combined-VMS-AIS-anchorages-296740e47d91806fa800f95812f65001
docker compose run pipeline name_anchorages \
        --job_name name-anchorages \
        --input_table anchorages.unnamed_anchorages_v20190816 \
        --output_table scratch_amanda_ttl_120.named_anchorages_v20260302 \
        --config ./src/pipe_anchorages/assets/config/name_anchorages_cfg.yaml \
        --labels resource_creator=amanda_lohmann \
        --labels project=ais-vms-anchorages \
        --labels stage=proof-of-concept \
        --labels step=named-ais-vms-anchorages \
        --max_num_workers 100 \
        --project world-fishing-827 \
        --staging_location gs://machine-learning-dev-ttl-120d-central1/anchorages \
        --temp_location gs://machine-learning-dev-ttl-120d-central1/temp \
        --setup_file ./setup.py \
        --runner DataflowRunner \
        --disk_size_gb 100 \
        --region us-central1

# In[5]:


from amanda_notebook_bq_helper import *


# In[4]:


named_anchorages_table = 'world-fishing-827.scratch_amanda_ttl_120.named_anchorages_v20260302'


# # run query to add extra fields

# In[11]:


q = f'''
CREATE OR REPLACE TABLE `{named_anchorages_table}`
AS 
--
-- add distance from shore to
-- named anchorages
--
WITH add_distance_from_shore AS (
SELECT 
  * except(gridcode)
FROM
  (
    SELECT *,
      format("lon:%+07.2f_lat:%+07.2f", round(lon/0.01)*0.01, round(lat/0.01)*0.01) as gridcode
    FROM `{named_anchorages_table}` 
    WHERE lat is NOT NULL AND lon IS NOT NULL
  )
LEFT JOIN 
  (
    SELECT
      gridcode,
      distance_from_shore_m
    FROM
    `pipe_static.spatial_measures`
  )
USING (gridcode))
--
-- Add dock/not-dock label
-- Also reorder columns to match current order.
--
SELECT lat, lon,  total_visits, drift_radius, top_destination,  unique_stationary_ssvid,  unique_stationary_fishing_ssvid,
unique_active_ssvid,  unique_total_ssvid, active_ssvid_days,  stationary_ssvid_days,  stationary_fishing_ssvid_days,  s2id,
label,  sublabel, label_source, iso3, distance_from_shore_m,  dock
 
FROM (
SELECT
*
FROM
add_distance_from_shore) a
LEFT JOIN
(SELECT
s2id AS s2id1, 
dock
FROM
`anchorages.anchorages_dock_label_v20191006`) b
ON a.s2id = b.s2id1
'''
run_bq(q)


# # delete 3 incorrect anchorages

# In[12]:


q = f'''
DELETE FROM `{named_anchorages_table}`
WHERE s2id IN ('50000001', '5aaaaaab', '140edc01');
'''
run_bq(q)


# In[4]:


# where are these anchorages?
from amanda_anchorage_helper import *
print(s2id_to_latlon('50000001'))
print(s2id_to_latlon('5aaaaaab'))
print(s2id_to_latlon('140edc01'))


# In[ ]:





# # remove duplicates that somehow survived

# In[ ]:





# In[13]:


q = f'''
CREATE OR REPLACE TABLE `{named_anchorages_table}` AS

SELECT * 
FROM `{named_anchorages_table}`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY s2id
  ORDER BY sublabel IS NULL, sublabel
) = 1;
'''
run_bq(q)


# # add Nate's labels

# I had Gemini comment this and it identified that the Kenya step is being skipped. I'm not going to fix this for now but will deal with it later

# In[ ]:


q = f'''
CREATE OR REPLACE TABLE `{named_anchorages_table}` AS

-- 1. DEFINE THE SOURCE OF TRUTH FOR LABELS
-- This CTE pulls the "master" list of correct s2ids, labels, and iso3 codes.
WITH good_labels AS (
  SELECT
    point_id As s2id,
    port_label AS label,
    point_label AS sublabel,
    iso3 AS iso3
  FROM
    `world-fishing-827.scratch_nate.final_gfw_ports_with_ids_v20220421`
),

-- 2. ATTACH STATISTICS TO MASTER LABELS
-- This takes the "good labels" and joins them back to the original table 
-- to retrieve the statistical columns (lat, lon, visits, drift_radius, etc.).
-- It ensures we keep the stats but use the standardized names.
updated_labels AS (
  SELECT
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

-- 3. 
-- This retrieves anchorages that exist in the named anchorages table
-- but were missing from the "good_labels" master list.
-- It excludes specific IDs to avoid duplicates.
-- Note: In the original query, drawing from the OG table may have only restored Antarctica. Now it pulls in all new anchorages added since Nate's rename query was written
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
  WHERE s2id NOT IN ('140edc01','5aaaaaab','50000001') 
  AND s2id NOT IN (SELECT s2id FROM updated_labels)
),

-- 4. COMBINE MAIN DATASETS
-- Merges the QAed label list with the anchorages only in named anchorages but not QAed label list
final AS (
  SELECT 
    s2id, label, sublabel, iso3, lat, lon, total_visits, drift_radius,
    unique_stationary_ssvid, unique_stationary_fishing_ssvid, unique_active_ssvid,
    unique_total_ssvid, active_ssvid_days, stationary_ssvid_days,
    stationary_fishing_ssvid_days, distance_from_shore_m, top_destination,
    label_source, dock
  FROM updated_labels

  UNION ALL

  SELECT * FROM missing_antarctic_anchorages
),

-- 5. LOAD PORT DATABASE
-- Helper CTE to load the ports database for the Abidjan lookup.
gfw_ports_database AS (
    SELECT *
    FROM `world-fishing-827.proj_pew_ports.gfw_ports_database_v20230424`
),

-------------------------------------------------------
-- START REGIONAL OVERRIDES
-- The following steps sequentially update labels for specific regions.
-------------------------------------------------------

-- A. ABIDJAN UPDATE
-- Finds Abidjan ports in the ports database to get the cluster_label.
merge_abidjan AS (
    SELECT 
      * FROM 
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
    -- If a new sublabel exists in the override, use it; otherwise keep the old one.
    IF(sublabel2 IS NULL, sublabel, sublabel2) AS sublabel,
    iso3, lat, lon, total_visits, drift_radius, unique_stationary_ssvid,
    unique_stationary_fishing_ssvid, unique_active_ssvid, unique_total_ssvid,
    active_ssvid_days, stationary_ssvid_days, stationary_fishing_ssvid_days,
    distance_from_shore_m, top_destination,
    -- If we updated the sublabel, change source to 'tmt'; otherwise keep original.
    IF(sublabel2 IS NOT NULL, 'tmt', label_source) AS label_source,
    dock
  FROM
    merge_abidjan
),

-- B. SENEGAL UPDATE
-- Uses `sen_tmt_anchorage_labels` to override Senegal sublabels.
merge_senegal AS (
    SELECT 
      * FROM 
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
    s2id, label,
    IF(sublabel2 IS NULL, sublabel, sublabel2) AS sublabel,
    iso3, lat, lon, total_visits, drift_radius, unique_stationary_ssvid,
    unique_stationary_fishing_ssvid, unique_active_ssvid, unique_total_ssvid,
    active_ssvid_days, stationary_ssvid_days, stationary_fishing_ssvid_days,
    distance_from_shore_m, top_destination,
    IF(sublabel2 IS NOT NULL, 'tmt', label_source) AS label_source,
    dock
  FROM
    merge_senegal
),

-- C. GUINEA UPDATE
-- Uses `gin_tmt_anchorage_labels` to override Guinea sublabels.
merge_guinea AS (
    SELECT 
      * FROM 
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
    s2id, label,
    IF(sublabel2 IS NULL, sublabel, sublabel2) AS sublabel,
    iso3, lat, lon, total_visits, drift_radius, unique_stationary_ssvid,
    unique_stationary_fishing_ssvid, unique_active_ssvid, unique_total_ssvid,
    active_ssvid_days, stationary_ssvid_days, stationary_fishing_ssvid_days,
    distance_from_shore_m, top_destination,
    IF(sublabel2 IS NOT NULL, 'tmt', label_source) AS label_source,
    dock
  FROM
    merge_guinea
),

-- D. KENYA UPDATE
-- !!! WARNING: This step currently joins the GUINEA table (`gin_tmt...`), 
-- likely a copy-paste error. It should probably be a Kenya table.
merge_kenya AS (
    SELECT 
      * FROM 
      update_guinea 
    LEFT JOIN
      (SELECT
        s2id,
        sublabel AS sublabel2
      FROM
        `world-fishing-827.scratch_nate.gin_tmt_anchorage_labels` -- <--- CHECK THIS TABLE
      )
    USING (s2id)
),

update_kenya AS (
  SELECT 
    s2id, label,
    IF(sublabel2 IS NULL, sublabel, sublabel2) AS sublabel,
    iso3, lat, lon, total_visits, drift_radius, unique_stationary_ssvid,
    unique_stationary_fishing_ssvid, unique_active_ssvid, unique_total_ssvid,
    active_ssvid_days, stationary_ssvid_days, stationary_fishing_ssvid_days,
    distance_from_shore_m, top_destination,
    IF(sublabel2 IS NOT NULL, 'tmt', label_source) AS label_source,
    dock
  FROM
    merge_kenya
),

-- E. GHANA UPDATE
-- !!! WARNING: This step selects from `update_guinea`, skipping `update_kenya`.
-- This means any changes made in the Kenya step are DROPPED.
-- Change `FROM update_guinea` to `FROM update_kenya`.
merge_ghana AS (
    SELECT 
      * FROM 
      update_guinea  -- <--- BROKEN CHAIN: Should likely be `update_kenya`
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
    s2id, label,
    IF(sublabel2 IS NULL, sublabel, sublabel2) AS sublabel,
    iso3, lat, lon, total_visits, drift_radius, unique_stationary_ssvid,
    unique_stationary_fishing_ssvid, unique_active_ssvid, unique_total_ssvid,
    active_ssvid_days, stationary_ssvid_days, stationary_fishing_ssvid_days,
    distance_from_shore_m, top_destination,
    IF(sublabel2 IS NOT NULL, 'tmt', label_source) AS label_source,
    dock
  FROM
    merge_ghana
)

-- 6. FINAL OUTPUT
-- Final cleanup: replaces the word "ANCHORAGE" with "OFFSHORE" in sublabels.
SELECT 
  s2id,
  label,
  REPLACE(sublabel, 'ANCHORAGE', 'OFFSHORE') AS sublabel,
  iso3, lat, lon, total_visits, drift_radius,
  unique_stationary_ssvid, unique_stationary_fishing_ssvid, unique_active_ssvid,
  unique_total_ssvid, active_ssvid_days, stationary_ssvid_days,
  stationary_fishing_ssvid_days, distance_from_shore_m, top_destination,
  label_source,
  dock 
FROM 
  update_ghana
'''
run_bq(q)


# # remove anchorage with meaningless lat/lon that re-appears from Nate's code 

# I removed this anchorage from the overrides csv because its lat/lon were entered incorrectly (I think lon = 1001 or something) and it caused the anchorage to have an s2id in the wrong place. But running Nate's rename query adds it back in. This anchorage is labeled as POINTE NOIRE, COG but it appears in the Caribbean

# In[ ]:


q = f'''
DELETE FROM `{named_anchorages_table}`
WHERE s2id = "8efe7543"
'''


# # replace distance-from-shore with updated values

# In[6]:


q = f'''
CREATE OR REPLACE TABLE `{named_anchorages_table}` AS

WITH spatial_measures AS (
  SELECT 
    gridcode,
    distance_from_shore_m
  FROM `world-fishing-827.scratch_tyler.spatial_measures_clustered_v20260403`
)

SELECT 
  anch.* EXCEPT(distance_from_shore_m), -- Drop the old column
  sm.distance_from_shore_m            -- Add the new column
FROM `{named_anchorages_table}` AS anch
LEFT JOIN spatial_measures AS sm
  ON format("lon:%+07.2f_lat:%+07.2f", FLOOR(anch.lon*100)/100, FLOOR(anch.lat*100)/100) = sm.gridcode;
'''

run_bq(q)


# # checks

# ## duplicates

# In[4]:


q = f'''
SELECT *
FROM `{named_anchorages_table}`
QUALIFY COUNT(*) OVER(PARTITION BY s2id) > 1
'''
get_bq_df(q)


# ## check against existing named_anchorages table

# ### count of differences by country

# In[5]:


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
  COUNT(*) AS new_anchorages_count,
  label_source,
FROM
  compare
GROUP BY mismatch_location, iso3, label_source
ORDER BY new_anchorages_count DESC

'''
get_bq_df(q)


# In[3]:


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
WHERE mismatch_location = 'Only in global-fishing-watch'

'''
get_bq_df(q)


# In[ ]:





# ### label changes from old to new

# In[ ]:


q = f'''
SELECT
  dev.s2id,
  -- Show the conflicting values side-by-side
  dev.label AS dev_label,
  prod.label AS prod_label,
  dev.sublabel AS dev_sublabel,
  prod.sublabel AS prod_sublabel
FROM
  `{named_anchorages_table}` AS dev
INNER JOIN
  `global-fishing-watch.anchorages.named_anchorages` AS prod
USING
  (s2id)
WHERE
  -- Check for differences, handling NULLs safely
  IFNULL(dev.label, 'NULL_PLACEHOLDER') != IFNULL(prod.label, 'NULL_PLACEHOLDER')
  OR 
  IFNULL(dev.sublabel, 'NULL_PLACEHOLDER') != IFNULL(prod.sublabel, 'NULL_PLACEHOLDER')
'''
get_bq_df(q)


# ## distance-from-shore

# ### distribution

# In[ ]:


q = f'''
SELECT 
  MIN(distance_from_shore_m) AS min_dfs, 
  MAX(distance_from_shore_m) AS max_dfs,
  AVG(distance_from_shore_m) AS avg_dfs,
  -- Creates 2 buckets and takes the middle value (the 1st offset)
  APPROX_QUANTILES(distance_from_shore_m, 2)[OFFSET(1)] AS median_dfs
FROM `{named_anchorages_table}`
'''
get_bq_df(q)


# ### nulls

# In[7]:


q = f'''
SELECT 
  *
FROM `{named_anchorages_table}`
WHERE distance_from_shore_m IS NULL
'''
get_bq_df(q)

