import os
import posixpath as pp
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.decorators import apply_defaults

# Determine the location of the anchorages code to run
import pipe_anchorages
ANCHORAGES_PATH = os.path.dirname(os.path.dirname(pipe_anchorages.__file__))

# The default operator doesn't template options
class TemplatedDataFlowPythonOperator(DataFlowPythonOperator):
    template_fields = ['options']


CONNECTION_ID = 'google_cloud_default' 
PROJECT_ID='{{ var.value.GCP_PROJECT_ID }}'

DATASET_ID='{{ var.value.IDENT_DATASET }}'
SOURCE_DATASET='{{ var.value.IDENT_SOURCE_DATASET }}'

THIS_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DAG_FILES = THIS_SCRIPT_DIR


ANCHORAGE_TABLE = '{{ var.value.PORT_EVENTS_ANCHORAGE_TABLE }}'
INPUT_TABLE = '{{ var.value.PORT_EVENTS_INPUT_TABLE }}'
OUTPUT_TABLE = '{{ var.value.PORT_EVENTS_OUTPUT_TABLE }}'

# ANCHORAGE_TABLE = 'gfw_raw.anchorage_naming_20171026' # TODO: Should not be hardcoded here
# pipeline_classify_p_p429_resampling_2' # TODO: Should not be hardcoded here
# OUTPUT_TABLE='machine_learning_dev_ttl_30d.in_out_events_test_dag' # TODO: Should not be hardcoded here


# See note about logging in readme.md
LOG_DIR = pp.join(THIS_SCRIPT_DIR, 'logs')
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
NORMALIZED_LOG_FILE = pp.join(LOG_DIR, 'normalized_startup.log')


TODAY_TABLE='{{ ds_nodash }}' 
YESTERDAY_TABLE='{{ yesterday_ds_nodash }}'


BUCKET='{{ var.value.GCS_BUCKET }}'
GCS_TEMP_DIR='gs://%s/dataflow-temp' % BUCKET
GCS_STAGING_DIR='gs://%s/dataflow-staging' % BUCKET


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 8, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': PROJECT_ID,
    'bigquery_conn_id': CONNECTION_ID,
    'gcp_conn_id': CONNECTION_ID,
    'write_disposition': 'WRITE_TRUNCATE',
    'allow_large_results': True,
}


@apply_defaults
def full_table (project_id, dataset_id, table_id, **kwargs):
    return '%s:%s.%s' % (project_id, dataset_id, table_id)

@apply_defaults
def table_sensor(task_id, table_id, dataset_id, dag, **kwargs):
    return BigQueryTableSensor(
        task_id=task_id,
        table_id=table_id,
        dataset_id=dataset_id,
        poke_interval=0,
        timeout=10,
        dag=dag
    )


with DAG('port_events_v0_1',  schedule_interval=timedelta(days=1), max_active_runs=3, default_args=default_args) as dag:

    yesterday_exists = table_sensor(task_id='yesterday_exists', dataset_id=INPUT_TABLE,
                                table_id=YESTERDAY_TABLE, dag=dag)

    today_exists = table_sensor(task_id='today_exists', dataset_id=INPUT_TABLE,
                                table_id=TODAY_TABLE, dag=dag)

    python_target = pp.join(ANCHORAGES_PATH, 'port_events_stub.py')
    logging.info("target: %s", python_target)

    # Note: task_id must use '-' instead of '_' because it gets used to create the dataflow job name, and
    # only '-' is allowed
    find_port_events=TemplatedDataFlowPythonOperator(
        task_id='issue-10-port-events',
        py_file=python_target,
        options={
            'project': PROJECT_ID,
            'anchorage-table': ANCHORAGE_TABLE,
            'start-date': '{{ ds }}',
            'end-date': '{{ ds }}',
            'input-table': INPUT_TABLE,
            'output-table': OUTPUT_TABLE,
            'staging_location': GCS_STAGING_DIR,
            'temp_location': GCS_TEMP_DIR,
            'max_num_workers': '100',
            'disk_size_gb': '50',
            'startup_log_file': NORMALIZED_LOG_FILE,
        },
        dag=dag
    )


    yesterday_exists >> today_exists >> find_port_events
