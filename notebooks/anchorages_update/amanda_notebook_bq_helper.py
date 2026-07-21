# %%
import pandas as pd
from jinja2 import Template
from google.cloud import bigquery
from google.cloud import bigquery_storage
import os
import hashlib
import re

# %%
def get_cache_path(q):
    """Returns absolute filepath of cached query result (filetype = .pkl)

    Has cache folder hardcoded. 
    Takes sha256 of query text string and returns [cache_folder]/[hash of query text].pkl 
    
    Normalizes all whitespace in the query string by replacing all whitespace sequences with a single space, so shouldn't be sensitive to additional newlines, tabs, etc
    """
    cache_folder = '/Users/alohmann/Dropbox/GFW/bq-cache/' # hardcoded cache folder
    q = re.sub(r'\s+', ' ', q).strip() # normalize whitespace in query by replacing all whitespace with a single space
    string_bytes = q.encode('utf-8')
    hash_object = hashlib.sha256(string_bytes) # Compute the hash (SHA-256)
    hash_hex = hash_object.hexdigest() # Get the hexadecimal representation of the hash
    return(cache_folder+hash_hex+'.pkl')

# %%
def get_bq_df(q):
    """Gets query result as a pandas dataframe and stores in cache if it isn't already there
    
    First checks if cached result exists, if so reads the file and returns that, if not calls run_bq_df(q) and stores result in cache
    """
    cache_path = get_cache_path(q)
    if os.path.exists(cache_path): # if file already exists
        df = pd.read_pickle(cache_path)
        return(df)

    df = run_bq_df(q)
    
    cache_folder = os.path.dirname(cache_path)
    if os.path.isdir(cache_folder): # if the cache folder exists, cache it - if cache folder doesn't exist, just skip this step
        #so won't throw an error if run on a different machine without updating cache folder
        df.to_pickle(cache_path)
    else: 
      print("WARNING: get_bq_df() function: Cache folder hardcoded in amanda_notebook_bq_helper.py script does not exist, so BQ results are not being locally cached. You can ignore this if you didn't expect the results to be locally cached. (This is coming from Amanda's helper code for pulling from BQ into notebooks locally.)")

    return(df)

# %%
def run_bq_df(q):
    """Retrieves query result from BQ
    
    """
    client = bigquery.Client()
    bqstorage_client = bigquery_storage.BigQueryReadClient()
    df = client.query(q).to_dataframe(bqstorage_client=bqstorage_client)
    return(df)

# %%
def fresh_get_bq_df(q):
    """Gets query result as a pandas dataframe and replaces it in cache
    
    Calls run_bq_df(q) and stores result in cache, overwriting if it already exists
    """
    cache_path = get_cache_path(q)

    df = run_bq_df(q)
    
    cache_folder = os.path.dirname(cache_path)
    if os.path.isdir(cache_folder): # if the cache folder exists, cache it - if cache folder doesn't exist, just skip this step
        #so won't throw an error if run on a different machine without updating cache folder
        df.to_pickle(cache_path)

    return(df)


# %%
def get_query_size(q):
    """Prints query size
    
    """
    cache_path = get_cache_path(q)
    if os.path.exists(cache_path):
        print(f'''No data processed - local cached version exists''')
        return

    # Create a BigQuery client
    client = bigquery.Client()
    # Configure the dry run job
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    # Run the query as a dry run
    query_job = client.query(q, job_config=job_config)
    # Check the estimated number of bytes processed
    estimated_bytes = query_job.total_bytes_processed
    print(f"Estimated data processed: {estimated_bytes / (1024 ** 3):.3f} GB")

# %%
def run_bq(q):
    """Runs a query in BQ but doesn't return anything
    
    For queries that save or edit a table in BQ and don't retrieve anything
    """
    # Initialize BigQuery client
    client = bigquery.Client()
    # Run the query job
    query_job = client.query(q)
    # Wait for the query job to complete
    query_job.result()

# %%
def df_to_bq(df,destination_dataset,destination_table,v='',project_id = 'world-fishing-827',if_exists = 'fail'):
    """Saves a pandas dataframe to a BQ table
    
    """
    if len(v) > 0:
        destination = f'''{destination_dataset}.{destination_table}_v{v}'''
    else:
        destination = f'''{destination_dataset}.{destination_table}'''
    df.to_gbq(
        destination_table=destination,
        project_id=project_id,
        if_exists=if_exists  # Options: 'fail', 'replace', 'append'
    )

# %%



