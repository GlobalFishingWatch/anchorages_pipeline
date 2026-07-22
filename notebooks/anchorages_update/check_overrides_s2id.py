#!/usr/bin/env python
# coding: utf-8

# In[2]:


from amanda_notebook_bq_helper import *
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import sys
from amanda_map_helper import *
from amanda_anchorage_helper import *


# In[3]:


df = pd.read_csv('/Users/alohmann/Library/CloudStorage/Dropbox/GFW/github/anchorages_pipeline/src/pipe_anchorages/assets/data/port_lists/anchorage_overrides.csv')
df


# In[4]:


df['s2id_backcalc'] = df.apply(lambda row: s2_anchorage_style(row['latitude'], row['longitude']), axis=1)
df


# In[7]:


mismatched = df[df['s2id_backcalc'] != df['s2id']]
mismatched[['s2id','s2id_backcalc','latitude','longitude','label','sublabel','iso3']]


# In[8]:


map_s2_anchorages(mismatched)


# In[13]:


pn = df[df['label']=='POINTE NOIRE'].reset_index(drop=True)


# In[16]:


plt.scatter(pn['longitude'],pn['latitude'])
plt.xlim([0, 180])
plt.ylim([-10, 10])


# In[ ]:


s2id_to_latlon('8efe7543')

