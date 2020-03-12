from airflow import DAG
from airflow.models import Variable

from airflow_ext.gfw.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow_ext.gfw.operators.dataflow_operator import DataFlowDirectRunnerOperator

import logging
import posixpath as pp


PIPELINE = 'pipe_anchorages'


class PipeAnchoragesPortEventsDagFactory(DagFactory):

    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(PipeAnchoragesPortEventsDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def source_date(self):
        if schedule_interval!='@daily' and schedule_interval != '@monthly' and schedule_interval != '@yearly':
            raise ValueError('Unsupported schedule interval {}'.format(self.schedule_interval))

    def build(self, dag_id):
        config = self.config

        start_date, end_date = self.source_date_range()

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            source_sensors = self.source_table_sensors(dag)

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

            for source_exists in source_sensors:
                dag >> source_exists >> port_events >> ensure_creation_tables

            return dag


class PipeAnchoragesPortVisitsDagFactory(DagFactory):

    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(PipeAnchoragesPortVisitsDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def source_date(self):
        if schedule_interval!='@daily' and schedule_interval != '@monthly' and schedule_interval != '@yearly':
            raise ValueError('Unsupported schedule interval {}'.format(self.schedule_interval))

    def build(self, dag_id):
        config = self.config

        start_date, end_date = self.source_date_range()

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:

            source_exists = self.table_sensor(
                dag=dag,
                task_id='source_exists_{port_events_table}'.format(**config),
                project='{project_id}'.format(**config),
                dataset='{pipeline_dataset}'.format(**config),
                table='{port_events_table}'.format(**config),
                date=self.source_sensor_date_nodash()
            )

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



class PipeAnchoragesVoyagesDagFactory(DagFactory):

    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(PipeAnchoragesVoyagesDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def source_date(self):
        if schedule_interval!='@daily' and schedule_interval != '@monthly' and schedule_interval != '@yearly':
            raise ValueError('Unsupported schedule interval {}'.format(self.schedule_interval))

    def build(self, dag_id):
        config = self.config

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:

            source_exists = self.table_sensor(
                dag=dag,
                task_id='source_exists_{port_visits_table}'.format(**config),
                project='{project_id}'.format(**config),
                dataset='{pipeline_dataset}'.format(**config),
                table='{port_visits_table}'.format(**config),
                date=self.source_sensor_date_nodash()
            )

            voyage_generation = self.build_docker_task({
                'task_id':'anchorages_voyage_generation',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'anchorages-voyage-generation',
                'dag':dag,
                'arguments':['generate_voyages',
                             '{project_id}:{pipeline_dataset}'.format(**config),
                             '{port_visits_table}'.format(**config),
                             '{project_id}:{pipeline_dataset}.{voyages_table}'.format(**config)]
            })

            dag >> source_exists >> voyage_generation

            return dag

port_events_daily_dag = build_port_events_dag('port_events_daily', '@daily')
port_events_monthly_dag = build_port_events_dag('port_events_monthly', '@monthly')
port_events_yearly_dag = build_port_events_dag('port_events_yearly', '@yearly')

port_visits_daily_dag = PipeAnchoragesPortVisitsDagFactory(schedule_interval='@daily').build('port_visits_daily')
port_visits_monthly_dag = PipeAnchoragesPortVisitsDagFactory(schedule_interval='@monthly').build('port_visits_monthly')
port_visits_yearlys_dag = PipeAnchoragesPortVisitsDagFactory(schedule_interval='@yearly').build('port_visits_yearly')

pipe_anchorages_voyages_daily_dag = PipeAnchoragesVoyagesDagFactory(schedule_interval='@daily').build('pipe_anchorages_voyages_daily')
pipe_anchorages_voyages_monthly_dag = PipeAnchoragesVoyagesDagFactory(schedule_interval='@monthly').build('pipe_anchorages_voyages_monthly')
pipe_anchorages_voyages_yearlys_dag = PipeAnchoragesVoyagesDagFactory(schedule_interval='@yearly').build('pipe_anchorages_voyages_yearly')
