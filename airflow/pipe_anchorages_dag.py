import posixpath as pp
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

from pipe_tools.airflow.dataflow_operator import DataFlowDirectRunnerOperator
from pipe_tools.airflow.config import load_config
from pipe_tools.airflow.config import default_args


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
        retry_delay=timedelta(minutes=60)
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
                max_num_workers="100",
                disk_size_gb="50",
                requirements_file='./requirements.txt',
                setup_file='./setup.py'
            )
        )

        publish_events = BashOperator(
            task_id='publish_events',
            bash_command='{docker_run} {docker_image} publish_events '
                         '{date_range} '
                         '{project_id}:{pipeline_dataset}.{port_events_table} '
                         '{project_id}:{events_dataset}.{events_table}'.format(**config)
        )

        dag >> source_exists >> port_events >> publish_events

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
                max_num_workers="100",
                disk_size_gb="50",
                requirements_file='./requirements.txt',
                setup_file='./setup.py'
            )
        )

        source_exists >> port_visits

        return dag


port_events_daily_dag = build_port_events_dag('port_events_daily', '@daily')
port_events_monthly_dag = build_port_events_dag('port_events_monthly', '@monthly')

port_visits_daily_dag = build_port_visits_dag('port_visits_daily', '@daily')
port_visits_monthly_dag = build_port_visits_dag('port_visits_monthly', '@monthly')
