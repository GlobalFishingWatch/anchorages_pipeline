#!/usr/bin/env python
# coding: utf-8

# see notes here https://www.notion.so/globalfishingwatch/Combined-VMS-AIS-anchorages-296740e47d91806fa800f95812f65001
docker compose run pipeline name_anchorages \
        --job_name name-anchorages \
        --input_table anchorages.unnamed_anchorages_v20190816 \
        --output_table scratch_amanda_ttl_120.named_anchorages_v20260226 \
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

# In[9]:


from amanda_notebook_bq_helper import *


# In[10]:


named_anchorages_table = 'world-fishing-827.scratch_amanda_ttl_120.named_anchorages_v20260226'


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


# # remove duplicates that somehow survived

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


# # checks

# ## duplicates

# In[14]:


q = f'''
SELECT *
FROM `{named_anchorages_table}`
QUALIFY COUNT(*) OVER(PARTITION BY s2id) > 1
'''
get_bq_df(q)


# ## check against existing named_anchorages table

# In[15]:


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


# In[ ]:





# 
