import os
import posixpath as pp
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.decorators import apply_defaults




CONNECTION_ID = 'google_cloud_default'
PROJECT_ID='world-fishing-827'

THIS_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DAG_FILES = THIS_SCRIPT_DIR

import anchorages
ANCHORAGES_PATH = os.path.dirname(os.path.dirname(anchorages.__file__))

# jinja2_env = Environment(loader=FileSystemLoader(DAG_FILES, followlinks=True))

# See note about logging in readme.md
LOG_DIR = pp.join(THIS_SCRIPT_DIR, 'logs')
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
NORMALIZED_LOG_FILE = pp.join(LOG_DIR, 'normalized_startup.log')


ANCHORAGE_TABLE = 'gfw_raw.anchorage_naming_20171026'

INPUT_TABLE='pipeline_classify_p_p429_resampling_2'
TODAY_TABLE='{{ ds_nodash }}' 
YESTERDAY_TABLE='{{ yesterday_ds_nodash }}'

OUTPUT_TABLE='machine_learning_dev_ttl_30d.in_out_events_test_dag'

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


with DAG('port_events_dag_9',  schedule_interval=timedelta(days=1), default_args=default_args) as dag:

    yesterday_exists = table_sensor(task_id='yesterday_exists', dataset_id=INPUT_TABLE,
                                table_id=YESTERDAY_TABLE, dag=dag)


    today_exists = table_sensor(task_id='today_exists', dataset_id=INPUT_TABLE,
                                table_id=TODAY_TABLE, dag=dag)


    # Note: task_id must use '-' instead of '_' because it gets used to create the dataflow job name, and
    # only '-' is allowed
    find_port_events=DataFlowPythonOperator(
        task_id='issue-18-port-events',
        py_file=pp.join(ANCHORAGES_PATH, 'port_events.py'),
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

                          #     --job-name portvisitsoneday \
                          # --anchorage-table gfw_raw.anchorage_naming_20171026 \
                          # --start-date 2016-01-01 \
                          # --end-date 2016-01-01 \
                          # --output-table machine_learning_dev_ttl_30d.in_out_events_test \
                          # --project world-fishing-827 \
                          # --max_num_workers 100


    yesterday_exists >> today_exists >> find_port_events

