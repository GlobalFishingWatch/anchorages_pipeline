import posixpath as pp
from datetime import datetime, timedelta, date
import logging

from airflow import DAG
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.models import Variable

from airflow_ext.gfw.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow_ext.gfw.operators.dataflow_operator import DataFlowDirectRunnerOperator
from airflow_ext.gfw.config import load_config
from airflow_ext.gfw.config import default_args


CONFIG = load_config('pipe_anchorages')
DEFAULT_ARGS = default_args(CONFIG)


def table_sensor(dataset_id, table_id, date):
    return BigQueryTableSensor(
        task_id='source_exists',
        dataset_id=dataset_id,
        table_id='{}{}'.format(table_id, date),
        poke_interval=10,   # check every 10 seconds for a minute
        timeout=60,
        retries=24*7,       # retry once per hour for a week
        retry_delay=timedelta(minutes=60),
        retry_exponential_backoff=False
    )


def build_port_events_dag(dag_id, schedule_interval='@daily', extra_default_args=None, extra_config=None):

    default_args = DEFAULT_ARGS.copy()
    default_args.update(extra_default_args or {})

    config = CONFIG.copy()
    config.update(extra_config or {})

    if schedule_interval=='@daily':
        source_sensor_date = '{{ ds_nodash }}'
        start_date = '{{ ds }}'
        end_date = '{{ ds }}'
    elif schedule_interval == '@monthly':
        source_sensor_date = '{last_day_of_month_nodash}'.format(**config)
        start_date = '{first_day_of_month}'.format(**config)
        end_date = '{last_day_of_month}'.format(**config)
    elif schedule_interval == '@yearly':
        source_sensor_date = '{last_day_of_year_nodash}'.format(**config)
        start_date = '{first_day_of_year}'.format(**config)
        end_date = '{last_day_of_year}'.format(**config)
    else:
        raise ValueError('Unsupported schedule interval {}'.format(schedule_interval))

    config['date_range'] = ','.join([start_date, end_date])

    with DAG(dag_id,  schedule_interval=schedule_interval, default_args=default_args) as dag:

        source_exists = table_sensor(
            dataset_id='{source_dataset}'.format(**config),
            table_id='{source_table}'.format(**config),
            date=source_sensor_date)

        python_target = Variable.get('DATAFLOW_WRAPPER_STUB')

        # Note: task_id must use '-' instead of '_' because it gets used to create the dataflow job name, and
        # only '-' is allowed
        port_events = DataFlowDirectRunnerOperator(
            task_id='port-events',
            pool='dataflow',
            py_file=python_target,
            options=dict(
                startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'),
                                         'pipe_anchorages/port-events.log'),
                command='{docker_run} {docker_image} port_events'.format(**config),
                project=config['project_id'],
                runner='{dataflow_runner}'.format(**config),
                start_date=start_date,
                end_date=end_date,
                anchorage_table='{project_id}:{anchorage_table}'.format(**config),
                input_table='{source_dataset}.{source_table}'.format(**config),
                output_table='{pipeline_dataset}.{port_events_table}'.format(**config),
                temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                max_num_workers='{dataflow_max_num_workers}'.format(**config),
                disk_size_gb='{dataflow_disk_size_gb}'.format(**config),
                requirements_file='./requirements.txt',
                setup_file='./setup.py',
                start_padding='{port_events_start_padding}'.format(**config)
            )
        )

        ensure_creation_tables = BigQueryCreateEmptyTableOperator(
            task_id='ensure_port_events_creation_tables',
            dataset_id='{pipeline_dataset}'.format(**config),
            table_id='{port_events_table}'.format(**config),
            schema_fields=[
                {"name": "vessel_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
                {"name": "lat", "type":"FLOAT", "mode": "REQUIRED"},
                {"name": "lon", "type":"FLOAT", "mode": "REQUIRED"},
                {"name": "vessel_lat", "type":"FLOAT", "mode": "REQUIRED"},
                {"name": "vessel_lon", "type":"FLOAT", "mode": "REQUIRED"},
                {"name": "anchorage_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "event_type", "type": "STRING", "mode": "REQUIRED"}
            ],
            start_date_str=start_date,
            end_date_str=end_date
        )

        dag >> source_exists >> port_events >> ensure_creation_tables

        return dag


def build_port_visits_dag(dag_id, schedule_interval='@daily', extra_default_args=None, extra_config=None):

    default_args = DEFAULT_ARGS.copy()
    default_args.update(extra_default_args or {})

    config = CONFIG.copy()
    config.update(extra_config or {})

    if schedule_interval=='@daily':
        source_sensor_date = '{{ ds_nodash }}'
        start_date = '{{ ds }}'
        end_date = '{{ ds }}'
    elif schedule_interval == '@monthly':
        source_sensor_date = '{last_day_of_month_nodash}'.format(**config)
        start_date = '{first_day_of_month}'.format(**config)
        end_date = '{last_day_of_month}'.format(**config)
    elif schedule_interval == '@yearly':
        source_sensor_date = '{last_day_of_year_nodash}'.format(**config)
        start_date = '{first_day_of_year}'.format(**config)
        end_date = '{last_day_of_year}'.format(**config)
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
        port_visits = DataFlowDirectRunnerOperator(
            task_id='port-visits',
            pool='dataflow',
            py_file=python_target,
            options=dict(
                startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'),
                                         'pipe_anchorages/port-visits.log'),
                command='{docker_run} {docker_image} port_visits'.format(**config),
                project=config['project_id'],
                runner='{dataflow_runner}'.format(**config),
                start_date=start_date,
                end_date=end_date,
                events_table='{project_id}:{pipeline_dataset}.{port_events_table}'.format(**config),
                start_padding='{port_visits_start_padding}'.format(**config),
                output_table='{pipeline_dataset}.{port_visits_table}'.format(**config),
                temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                max_num_workers='{dataflow_max_num_workers}'.format(**config),
                disk_size_gb='{dataflow_disk_size_gb}'.format(**config),
                requirements_file='./requirements.txt',
                setup_file='./setup.py'
            )
        )

        ensure_creation_tables = BigQueryCreateEmptyTableOperator(
            task_id='ensure_port_visits_creation_tables',
            dataset_id='{pipeline_dataset}'.format(**config),
            table_id='{port_visits_table}'.format(**config),
            schema_fields=[
                { "mode": "REQUIRED", "name": "vessel_id", "type": "STRING" },
                { "mode": "REQUIRED", "name": "start_timestamp", "type": "TIMESTAMP" },
                { "mode": "REQUIRED", "name": "start_lat", "type": "FLOAT" },
                { "mode": "REQUIRED", "name": "start_lon", "type": "FLOAT" },
                { "mode": "REQUIRED", "name": "start_anchorage_id", "type": "STRING" },
                { "mode": "REQUIRED", "name": "end_timestamp", "type": "TIMESTAMP" },
                { "mode": "REQUIRED", "name": "end_lat", "type": "FLOAT" },
                { "mode": "REQUIRED", "name": "end_lon", "type": "FLOAT" },
                { "mode": "REQUIRED", "name": "end_anchorage_id", "type": "STRING" },
                { "fields": [
                    { "mode": "REQUIRED", "name": "vessel_id", "type": "STRING" },
                    { "mode": "REQUIRED", "name": "timestamp", "type": "TIMESTAMP" },
                    { "mode": "REQUIRED", "name": "lat", "type": "FLOAT" },
                    { "mode": "REQUIRED", "name": "lon", "type": "FLOAT" },
                    { "mode": "REQUIRED", "name": "vessel_lat", "type": "FLOAT" },
                    { "mode": "REQUIRED", "name": "vessel_lon", "type": "FLOAT" },
                    { "mode": "REQUIRED", "name": "anchorage_id", "type": "STRING" },
                    { "mode": "REQUIRED", "name": "event_type", "type": "STRING" }
                ],
                "mode": "REPEATED", "name": "events", "type": "RECORD" }
            ],
            start_date_str=start_date,
            end_date_str=end_date
        )

        dag >> source_exists >> port_visits >> ensure_creation_tables

        return dag


port_events_daily_dag = build_port_events_dag('port_events_daily', '@daily')
port_events_monthly_dag = build_port_events_dag('port_events_monthly', '@monthly')
port_events_yearly_dag = build_port_events_dag('port_events_yearly', '@yearly')

port_visits_daily_dag = build_port_visits_dag('port_visits_daily', '@daily')
port_visits_monthly_dag = build_port_visits_dag('port_visits_monthly', '@monthly')
port_visits_yearlys_dag = build_port_visits_dag('port_visits_yearly', '@yearly')

