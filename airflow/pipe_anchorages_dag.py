from airflow import DAG
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.models import Variable

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory
from airflow_ext.gfw.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow_ext.gfw.operators.dataflow_operator import DataFlowDirectRunnerOperator

from datetime import timedelta

import logging
import posixpath as pp
import uuid




PIPELINE = 'pipe_anchorages'


class AnchorageDagFactory(DagFactory):

    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(AnchorageDagFactory, self).__init__(pipeline=pipeline, **kwargs)
        self.python_target = Variable.get('DATAFLOW_WRAPPER_STUB')

class PortEventsDagFactory(AnchorageDagFactory):

    def __init__(self, **kwargs):
        super(PortEventsDagFactory, self).__init__(**kwargs)

    def source_date(self):
        if schedule_interval!='@daily' and schedule_interval != '@monthly' and schedule_interval != '@yearly':
            raise ValueError(f'Unsupported schedule interval {self.schedule_interval}')

    def build(self, dag_id):
        config = self.config

        start_date, end_date = self.source_date_range()

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            source_sensors = self.source_table_sensors(dag)

            # Note: task_id must use '-' instead of '_' because it gets used to create the dataflow job name, and
            # only '-' is allowed
            port_events = DataFlowDirectRunnerOperator(
                task_id='port-events',
                pool='dataflow',
                py_file=self.python_target,
                options=dict(
                    # Airflow
                    startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'),
                                             'pipe_anchorages/port-events.log'),
                    command='{docker_run} {docker_image} port_events'.format(**config),
                    runner='{dataflow_runner}'.format(**config),

                    # Required
                    anchorage_table='{anchorage_table}'.format(**config),
                    input_table='{source_dataset}.{source_table}'.format(**config),
                    state_table='{pipeline_dataset}.{port_events_state_table}'.format(**config),
                    output_table='{pipeline_dataset}.{port_events_table}'.format(**config),
                    start_date=start_date,
                    end_date=end_date,

                    # GoogleCloud Option
                    project=config['project_id'],
                    temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                    staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                    region='{region}'.format(**config),

                    # Worker Option
                    max_num_workers='{dataflow_max_num_workers}'.format(**config),
                    disk_size_gb='{dataflow_disk_size_gb}'.format(**config),

                    # Setup Option
                    requirements_file='./requirements-worker-frozen.txt',
                    setup_file='./setup.py'
                )
            )

            ensure_creation_event_tables = BigQueryCreateEmptyTableOperator(
                task_id='ensure_port_events_creation_tables',
                dataset_id='{pipeline_dataset}'.format(**config),
                table_id='{port_events_table}'.format(**config),
                schema_fields=[
                    {"name": "seg_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "lat", "type":"FLOAT", "mode": "REQUIRED"},
                    {"name": "lon", "type":"FLOAT", "mode": "REQUIRED"},
                    {"name": "vessel_lat", "type":"FLOAT", "mode": "NULLABLE"},
                    {"name": "vessel_lon", "type":"FLOAT", "mode": "NULLABLE"},
                    {"name": "anchorage_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "last_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"}
                ],
                start_date_str=start_date,
                end_date_str=end_date
            )

            ensure_creation_state_tables = BigQueryCreateEmptyTableOperator(
                task_id='ensure_port_state_creation_tables',
                dataset_id='{pipeline_dataset}'.format(**config),
                table_id='{port_events_state_table}'.format(**config),
                schema_fields=[
                    {"name": "seg_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "date", "type": "DATE", "mode": "REQUIRED"},
                    {"name": "state", "type":"STRING", "mode": "NULLABLE"},
                    {"name": "active_port", "type":"STRING", "mode": "NULLABLE"},
                    {"name": "last_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"}
                ],
                start_date_str=start_date,
                end_date_str=end_date
            )

            for source_exists in source_sensors:
                dag >> source_exists >> port_events
            port_events >> ensure_creation_event_tables
            port_events >> ensure_creation_state_tables

            return dag


class PortVisitsDagFactory(AnchorageDagFactory):

    def __init__(self, **kwargs):
        super(PortVisitsDagFactory, self).__init__(**kwargs)
        self.config["temp_table_id"] = str(uuid.uuid4()).replace("-","_")

    def source_date(self):
        if schedule_interval!='@daily':
            raise ValueError(f'Unsupported schedule interval {self.schedule_interval}')

    def build(self, dag_id):
        config = self.config

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:

            source_exists = self.table_sensor(
                dag=dag,
                task_id='source_exists_{port_events_table}'.format(**config),
                project='{project_id}'.format(**config),
                dataset='{pipeline_dataset}'.format(**config),
                table='{port_events_table}'.format(**config),
                date=self.source_sensor_date_nodash()
            )

            segment_info_exists = BigQueryCheckOperator(
                task_id='segment_info_exists',
                sql='SELECT count(*) FROM `{source_dataset}.{segment_info_table}`'.format(**config),
                use_legacy_sql=False,
                retries=3,
                retry_delay=timedelta(minutes=30),
                max_retry_delay=timedelta(minutes=30),
                on_failure_callback=config_tools.failure_callback_gfw
            )

            overlappingandshort_segments_exists = BigQueryCheckOperator(
                task_id='overlappingandshort_segments_exists',
                sql='SELECT count(DISTINCT seg_id) FROM `{research_aggregated_segments_table}` WHERE overlapping_and_short and date(last_timestamp) = "{ds}"'.format(**config),
                use_legacy_sql=False,
                retries=144,
                retry_delay=timedelta(minutes=30),
                max_retry_delay=timedelta(minutes=30),
                on_failure_callback=config_tools.failure_callback_gfw
            )

            aux_table = f'{config["temp_dataset"]}.{config["temp_table_id"]}'
            # Note: task_id must use '-' instead of '_' because it gets used to create the dataflow job name, and
            # only '-' is allowed
            port_visits = DataFlowDirectRunnerOperator(
                task_id='port-compat-visits',
                pool='dataflow',
                py_file=self.python_target,
                options=dict(
                    # Airflow
                    startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'),
                                             'pipe_anchorages/port-visits.log'),
                    command='{docker_run} {docker_image} port_visits'.format(**config),
                    runner='{dataflow_runner}'.format(**config),

                    # Required
                    events_table='{pipeline_dataset}.{port_events_table}'.format(**config),
                    vessel_id_table='{source_dataset}.{segment_info_table}'.format(**config),
                    output_table=f'{aux_table}',
                    start_date=self.default_args['start_date'].strftime("%Y-%m-%d"),
                    end_date=f'{config["ds"]}',

                    # Optional
                    bad_segs_table='(SELECT DISTINCT seg_id FROM {research_aggregated_segments_table} WHERE overlapping_and_short)'.format(**config),
                    compat_output_table='{pipeline_dataset}.{port_visits_compatibility_table}'.format(**config),

                    # GoogleCloud Option
                    project=config['project_id'],
                    temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                    staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                    region='{region}'.format(**config),

                    #Worker Option
                    max_num_workers='{dataflow_max_num_workers}'.format(**config),
                    disk_size_gb='{dataflow_disk_size_gb}'.format(**config),

                    # Setup Option
                    requirements_file='./requirements-worker-frozen.txt',
                    setup_file='./setup.py'
                )
            )

            replaces_raw_port_visits = self.build_docker_task({
                'task_id':'replaces_raw_port_visits',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'replaces-raw-port-visits',
                'dag':dag,
                'arguments':['replaces_table',
                             '--project_id',
                             f'{config["project_id"]}',
                             '--from_table',
                             f'{aux_table}',
                             '--to_table',
                             '{pipeline_dataset}.{port_visits_table}'.format(**config)]
            })

            # ensure_creation_tables = BigQueryCreateEmptyTableOperator(
            #     task_id='ensure_port_visits_creation_tables',
            #     dataset_id='{pipeline_dataset}'.format(**config),
            #     table_id='{port_visits_compatibility_table}'.format(**config),
            #     schema_fields=[
            #         { "mode": "REQUIRED", "name": "vessel_id", "type": "STRING" },
            #         { "mode": "REQUIRED", "name": "start_timestamp", "type": "TIMESTAMP" },
            #         { "mode": "REQUIRED", "name": "start_lat", "type": "FLOAT" },
            #         { "mode": "REQUIRED", "name": "start_lon", "type": "FLOAT" },
            #         { "mode": "REQUIRED", "name": "start_anchorage_id", "type": "STRING" },
            #         { "mode": "REQUIRED", "name": "end_timestamp", "type": "TIMESTAMP" },
            #         { "mode": "REQUIRED", "name": "end_lat", "type": "FLOAT" },
            #         { "mode": "REQUIRED", "name": "end_lon", "type": "FLOAT" },
            #         { "mode": "REQUIRED", "name": "end_anchorage_id", "type": "STRING" },
            #         { "fields": [
            #             { "mode": "REQUIRED", "name": "vessel_id", "type": "STRING" },
            #             { "mode": "REQUIRED", "name": "timestamp", "type": "TIMESTAMP" },
            #             { "mode": "REQUIRED", "name": "lat", "type": "FLOAT" },
            #             { "mode": "REQUIRED", "name": "lon", "type": "FLOAT" },
            #             { "mode": "REQUIRED", "name": "vessel_lat", "type": "FLOAT" },
            #             { "mode": "REQUIRED", "name": "vessel_lon", "type": "FLOAT" },
            #             { "mode": "REQUIRED", "name": "anchorage_id", "type": "STRING" },
            #             { "mode": "REQUIRED", "name": "event_type", "type": "STRING" }
            #         ],
            #         "mode": "REPEATED", "name": "events", "type": "RECORD" }
            #     ],
            #     start_date_str=self.default_args['start_date'].strftime("%Y-%m-%d"),
            #     end_date_str=f'{config["ds"]}'
            # )

            voyage_generation = self.build_docker_task({
                'task_id':'voyage_compat_generation',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'voyage-compat-generation',
                'dag':dag,
                'arguments':['generate_voyages',
                             '{project_id}:{pipeline_dataset}'.format(**config),
                             '{port_visits_compatibility_table}'.format(**config),
                             '{project_id}:{pipeline_dataset}.{voyages_compatibility_table}'.format(**config)]
            })

            voyage_c2_generation = self.build_docker_task({
                'task_id':'voyage_c2_generation',
                'pool':'voyages_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'voyage-c2-generation',
                'dag':dag,
                'arguments':['generate_confidence_voyages',
                             '{project_id}:{pipeline_dataset}.{port_visits_table}'.format(**config),
                             '2'.format(**config),
                             '{project_id}:{pipeline_dataset}.{voyages_table}_c2'.format(**config)]
            })

            voyage_c3_generation = self.build_docker_task({
                'task_id':'voyage_c3_generation',
                'pool':'voyages_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'voyage-c3-generation',
                'dag':dag,
                'arguments':['generate_confidence_voyages',
                             '{project_id}:{pipeline_dataset}.{port_visits_table}'.format(**config),
                             '3'.format(**config),
                             '{project_id}:{pipeline_dataset}.{voyages_table}_c3'.format(**config)]
            })

            voyage_c4_generation = self.build_docker_task({
                'task_id':'voyage_c4_generation',
                'pool':'voyages_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'voyage-c4-generation',
                'dag':dag,
                'arguments':['generate_confidence_voyages',
                             '{project_id}:{pipeline_dataset}.{port_visits_table}'.format(**config),
                             '4'.format(**config),
                             '{project_id}:{pipeline_dataset}.{voyages_table}_c4'.format(**config)]
            })

            dag >> source_exists >> port_visits
            dag >> segment_info_exists >> port_visits
            dag >> overlappingandshort_segments_exists >> port_visits

            # port_visits >> ensure_creation_tables >> voyage_generation
            port_visits >> voyage_generation

            port_visits >> replaces_raw_port_visits

            replaces_raw_port_visits >> voyage_c2_generation
            replaces_raw_port_visits >> voyage_c3_generation
            replaces_raw_port_visits >> voyage_c4_generation

            return dag



for mode in ['daily', 'monthly', 'yearly']:
    globals()[f'port_events_{mode}'] = PortEventsDagFactory(schedule_interval=f'@{mode}').build(f'port_events_{mode}')
globals()[f'port_visits_voyages_daily'] = PortVisitsDagFactory(schedule_interval=f'@daily').build(f'port_visits_voyages_daily')
