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
from airflow.models import Variable


# # The default operator doesn't template options
# class TemplatedDataFlowPythonOperator(DataFlowPythonOperator):
#     template_fields = ['options']

config = Variable.get('pipe_anchorages', deserialize_json=True)
config['ds_nodash'] = '{{ ds_nodash }}'
config['first_day_of_month'] = '{{ execution_date.replace(day=1).strftime("%Y-%m-%d") }}'
config['last_day_of_month'] = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y-%m-%d") }}'
config['first_day_of_month_nodash'] = '{{ execution_date.replace(day=1).strftime("%Y%m%d") }}'
config['last_day_of_month_nodash'] = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y%m%d") }}'

GC_CONNECTION_ID = 'google_cloud_default' 
BQ_CONNECTION_ID = 'google_cloud_default'

# PROJECT_ID='{{ var.value.PROJECT_ID }}'

start_date_string = config['port_visits_start_date'].strip()
default_start_date = datetime.strptime(start_date_string, "%Y-%m-%d")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': default_start_date,
    'email': ['tim@globalfishingwatch.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': '{{ var.value.PROJECT_ID }}',
    'bigquery_conn_id': BQ_CONNECTION_ID,
    'gcp_conn_id': GC_CONNECTION_ID,
    'write_disposition': 'WRITE_TRUNCATE',
    'allow_large_results': True,
}



# @apply_defaults
# def table_sensor(task_id, table_id, dataset_id, dag, **kwargs):
#     return BigQueryTableSensor(
#         task_id=task_id,
#         table_id=table_id,
#         dataset_id=dataset_id,
#         poke_interval=0,
#         timeout=10,
#         dag=dag,
#         retry_delay=timedelta(minutes=60),
#         retries=24*7
#     )

def table_sensor(dataset_id, table_id, date):
    return BigQueryTableSensor(
        task_id='source_exists',
        dataset_id=dataset_id,
        table_id='{}{}'.format(table_id, date),
        poke_interval=10,   # check every 10 seconds for a minute
        timeout=60,
        retries=24*7,       # retry once per hour for a week
        retry_delay=timedelta(minutes=60)
    )


def build_dag(dag_id, schedule_interval):

    if schedule_interval=='@daily':
        source_sensor_date = '{{ ds_nodash }}'
        start_date = '{{ ds }}'
        end_date = '{{ ds }}'
    elif schedule_interval == '@monthly':
        source_sensor_date = '{last_day_of_month_nodash}'.format(**config)
        start_date = '{first_day_of_month}'.format(**config)
        end_date = '{last_day_of_month}'.format(**config)
    else:
        raise ValueError('Unsupported schedule interval {}'.format(schedule_interval))

    with DAG(dag_id,  schedule_interval=schedule_interval, default_args=default_args) as dag:

        source_exists = table_sensor(
            dataset_id='{pipeline_dataset}'.format(**config),
            table_id='{port_events_table}'.format(**config),
            date=source_sensor_date)

        python_target = Variable.get('DATAFLOW_WRAPPER_STUB')

        logging.info("target: %s", python_target)

        # Note: task_id must use '-' instead of '_' because it gets used to create the dataflow job name, and
        # only '-' is allowed
        port_visits = DataFlowPythonOperator(
            task_id='port-visits',
            depends_on_past=True,
            py_file=python_target,
            options=dict(
                startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'),
                                         'pipe_anchorages/port-events.log'),
                command='{docker_run} {docker_image} port_visits'.format(**config),
                project=config['project_id'],
                start_date=start_date,
                end_date=end_date,
                events_table='{project_id}:{pipeline_dataset}.{port_events_table}',
                start_padding='{port_visits_start_padding}'.format(**config),
                output_table='{project_id}:{pipeline_dataset}.{port_visits_table}'.format(**config),
                temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                max_num_workers="100",
                disk_size_gb="50",
                requirements_file='./requirements.txt',
                setup_file='./setup.py'
            )
        )

        # dataset, table_prefix = config['PORT_EVENTS_OUTPUT_TABLE'].split('.')
        #
        # table_id = '%s{{ ds_nodash }}' % table_prefix
        # source_exists = table_sensor(task_id='source_exists', dataset_id=dataset,
        #                             table_id=table_id, dag=dag)

        source_exists >> port_visits

        return dag


port_visits_daily_dag = build_dag('port_visits_daily', '@daily')
port_visits_monthly_dag = build_dag('port_visits_monthly', '@monthly')
