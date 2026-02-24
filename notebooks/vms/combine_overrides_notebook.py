# %%
from amanda_notebook_bq_helper import *
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
import json


from amanda_anchorage_helper import *
fig_fldr = './figures'
port_list_fldr = '../../src/pipe_anchorages/assets/data/port_lists'


# %% [markdown]
# # Merge anchorages_overrides with the vms country overrides

# %% [markdown]
# ## Read AIS anchorage overrides

# %%
ais_anchorages = pd.read_csv(f'{port_list_fldr}/anchorage_overrides.csv')
ais_anchorages['source'] = 'ais_anchorage_overrides'
ais_anchorages = clean_overrides(ais_anchorages,duplicate_option='keep_last')
ais_anchorages

# %% [markdown]
# ## VMS overrides

# %%
country_names = ['montenegro','brazil','chile','panama','ecuador','palau','papua_new_guinea','costa_rica']
override_lists = []
for c in country_names:
    print(f"\n{c}")
    df = pd.read_csv(f'{port_list_fldr}/{c}_vms_overrides.csv')
    df['source'] = f'{c}_vms_overrides'
    df = clean_overrides(df)
    override_lists.append(df)

c = 'vietnam'
print(f"\n{c}")
df = pd.read_csv(f'{port_list_fldr}/{c}_overrides.csv')
df['source'] = f'{c}_overrides'
df = clean_overrides(df)
override_lists.append(df)

# %% [markdown]
# ## Merge

# %%
combined_anchorages = override_lists[0]
for o in override_lists[1:]:
    combined_anchorages = pd.concat([combined_anchorages,o])
old_len = len(combined_anchorages)
duplicates = combined_anchorages[combined_anchorages.duplicated(subset='s2id', keep=False)]
combined_anchorages = combined_anchorages.drop_duplicates(subset='s2id', keep='first').reset_index(drop=True) # taking first because this takes ecuador over costa rica which has anchorages in ecuador

country_dupes = old_len - len(combined_anchorages)
if country_dupes > 0:
    print(f"WARNING: Dropped {country_dupes} duplicates from country-reviewed lists. This means one country-reviewed list overwrote at least 1 row from another.")
else:
    print("No S2 cells were present in 2 or more country lists")

combined_anchorages = pd.concat([combined_anchorages,ais_anchorages])
old_len = len(combined_anchorages)
combined_anchorages = combined_anchorages.drop_duplicates(subset='s2id', keep='last').reset_index(drop=True)
print(f"Dropped {old_len - len(combined_anchorages)} s2ids from AIS overrides list that were duplicated in the country-reviewed lists")

if country_dupes > 0:
    print('Country duplicates:')
    duplicates

# %%
combined_anchorages

# %%
combined_to_save = combined_anchorages.drop(columns=['source'])

combined_to_save.to_csv(f'{port_list_fldr}/combined_anchorage_overrides.csv',index=False)
combined_to_save
