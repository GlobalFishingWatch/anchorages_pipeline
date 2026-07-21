#!/usr/bin/env python
# coding: utf-8

# In[6]:


from amanda_notebook_bq_helper import *
from amanda_anchorage_helper import *
from amanda_map_helper import *


# In[11]:


lat, lon = 26.548654429356077, 50.032267649705524
label = 'Tarout Bay'
sublabel = ''
iso3 = 'SAU'
s2id = s2_anchorage_style(lat,lon)


# In[12]:


f"{s2id},{lat},{lon},{label},{sublabel},{iso3}"


# In[ ]:




