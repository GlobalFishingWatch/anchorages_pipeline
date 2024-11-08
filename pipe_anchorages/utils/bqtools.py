from datetime import datetime, timedelta
from jinja2 import Environment, FileSystemLoader
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import (BadRequest, Conflict as AlreadyExistErr)
import json, logging, sys

logger = logging.getLogger()

class BQTools:

    def __init__(self, project:str):
        self.project = project
        self.bq_client = bigquery.Client(project=project)

    def schema_json2builder(self, schema_path:str):
        """
        Reads json schema and convert to array of bigquery.SchemaFields.
        :param schema_path: The path to the schema.
        :type schema_path: str.
        """
        schema=None
        with open(schema_path) as schemafield:
            columns = json.load(schemafield)
            schema = list(map(lambda c:
                              bigquery.SchemaField(c['name'],c['type'],mode=c['mode'],description=c['description'], fields=([bigquery.SchemaField(f['name'],f['type'],mode=f['mode'],description=f['description']) for f in c['fields']] if c['type'].upper() == 'RECORD' else [])),
                              columns))
        return schema


    def create_tables_if_not_exists(self, destination_table:str, labels, table_desc:str, schema:list, clustering_fields:list=[], date_field:str='timestamp'):
        """Creates tables if they do not exists.
        If it doesn't exist, create it. And if exists, deletes the data of date range.

        :param destination_table: dataset.table of BQ.
        :type destination_table: str.
        :param labels: the label of the dataset. Default None.
        :type labels: dict.
        :param table_desc: the main description of the table.
        :type table_desc: str.
        :param schema: the schema of the table.
        :type schema: list[bigquery.SchemaField].
        :param clustering_fields: the clustering fields of the table.
        :type clustering_fields: list[str]. Default: [].
        :param date_field: the date field use to check the from and to dates.
        :type date_field: str. Default timestamp.
        """
        destination_table_ds, destination_table_tb = destination_table.split('.')
        destination_dataset_ref = bigquery.DatasetReference(self.bq_client.project, destination_table_ds)
        destination_table_ref = destination_dataset_ref.table(destination_table_tb)
        try:
            table = self.bq_client.get_table(destination_table_ref) #API request
            logger.info(f'Ensures the table [{table}] exists.')
            query_job = self.bq_client.query(
                f"""
                   DELETE FROM `{self.project}.{destination_table}`
                   WHERE date({date_field}) >= '1970-01-01' or {date_field} is null
                """,
                # f"""DROP TABLE `{self.project}.{ destination_table }`""",
                bigquery.QueryJobConfig(
                    use_query_cache=False,
                    use_legacy_sql=False,
                    labels=labels,
                )
            )
            logger.info(f'Delete Job {query_job.job_id} is currently in state {query_job.state}')
            result = query_job.result()
            # logger.info(f'Date range [{date_from:%Y-%m-%d},{date_to:%Y-%m-%d}] cleaned: {result}')
            logger.info(f'Table cleaned: {result}')

        except BadRequest as err:
            logger.error(f'Bad request received {err}.')

        except NotFound as err:
            table = bigquery.Table(destination_table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_ = bigquery.TimePartitioningType.MONTH,
                field = date_field,
            )
            table.require_partition_filter = True
            clustering_fields.insert(0, date_field)
            table.clustering_fields = clustering_fields
            table.description = table_desc
            table.labels = labels
            table = self.bq_client.create_table(table)
            logger.info(f'Table {destination_table_ds}.{destination_table_tb} created.')

        except Exception as err:
            logger.error(f'create_tables_if_not_exists - Unrecongnized error: {err}.')
            sys.exit(1)


    def create_table(self, destination_table:str, labels, table_desc:str, schema:list):
        """Creates the table if it does not exist.
        If it doesn't exist, create it. And if exists, returns error.

        :param destination_table: dataset.table of BQ.
        :type destination_table: str.
        :param labels: the label of the dataset. Default None.
        :type labels: dict.
        :param schema: the schema of the table.
        :type schema: list[bigquery.SchemaField].
        """
        destination_table_ds, destination_table_tb = destination_table.split('.')
        destination_dataset_ref = bigquery.DatasetReference(self.bq_client.project, destination_table_ds)
        destination_table_ref = destination_dataset_ref.table(destination_table_tb)
        try:
            table = bigquery.Table(destination_table_ref, schema=schema)
            table.description = table_desc
            table.labels = labels
            table = self.bq_client.create_table(table)
            logger.info(f'Table {destination_table_ds}.{destination_table_tb} created with specific schema.')
        except BadRequest as err:
            logger.error(f'Bad request received {err}.')
            sys.exit(1)
        except AlreadyExistErr as err:
            logger.warn(f'Already exists table: {err}.')
        except Exception as err:
            logger.error(f'create_table - Unrecongnized error: {err}.')
            sys.exit(1)

    def update_table(self, destination_table, description, schema):
        """Updates the schema of an existent table.

        :param destination_table: dataset.table of BQ.
        :type destination_table: str.
        :param description: the main description of the table.
        :type description: str.
        :param schema: the schema of the table.
        :type schema: list[bigquery.SchemaField].
        """
        destination_table_ds, destination_table_tb = destination_table.split('.')
        destination_dataset_ref = bigquery.DatasetReference(self.bq_client.project, destination_table_ds)
        destination_table_ref = destination_dataset_ref.table(destination_table_tb)
        try:
            table = self.bq_client.get_table(destination_table_ref)
            table.schema = schema
            table.description = description
            result = self.bq_client.update_table(table, ["description","schema"])
            logger.info(f'Update table schema from table {destination_table_ds}.{destination_table_tb}. Result: {result}')
        except BadRequest as err:
            logger.error(f'update_table - Bad request received {err}.')
            sys.exit(1)
        except Exception as err:
            logger.error(f'update_table - Unrecongnized error: {err}.')
            sys.exit(1)

    def update_table_descr(self, destination_table, description):
        """Updates the schema of an existent table.

        :param destination_table: dataset.table of BQ.
        :type destination_table: str.
        :param description: the main description of the table.
        :type description: str.
        """
        destination_table_ds, destination_table_tb = destination_table.split('.')
        destination_dataset_ref = bigquery.DatasetReference(self.bq_client.project, destination_table_ds)
        destination_table_ref = destination_dataset_ref.table(destination_table_tb)
        try:
            table = self.bq_client.get_table(destination_table_ref)
            table.description = description
            result = self.bq_client.update_table(table, ["description"])
            logger.info(f'Update table description from table {destination_table_ds}.{destination_table_tb}. Result: {result}')
        except BadRequest as err:
            logger.error(f'update_table_descr - Bad request received {err}.')
            sys.exit(1)
        except Exception as err:
            logger.error(f'update_table_descr - Unrecongnized error: {err}.')
            sys.exit(1)

    def run_estimation_query(self, query, destination, labels, is_partitioned:bool=True):
        self.run_query(query, destination, labels, True, is_partitioned)

    def run_query(self, query, destination, labels, estimate=False, is_partitioned:bool=True):
    # def run_query(self, query, destination, labels, estimate=False):
        """Runs the query using the client.

        :param query: The query.
        :type query: str.
        :param destination_table: dataset.table of BQ.
        :type destination_table: str.
        :param labels: the label of the dataset. Default None.
        :type labels: dict.
        :param estimate: If wants to get the estimation of the query.
        :type estimate: bool.
        """
        job_config = bigquery.QueryJobConfig(
            dry_run=estimate,
            use_query_cache=False,
            priority=bigquery.QueryPriority.BATCH,
            use_legacy_sql=False,
            write_disposition='WRITE_APPEND' if is_partitioned else 'WRITE_TRUNCATE',
            destination=destination,
            labels=labels,
        )

        logger.info(f'Execute {("estimate" if estimate else "real")} BATCH query, destination {destination}')
        if not estimate:
            logger.info(f'=====QUERY STARTS======\n{query}\n====QUERY ENDS====')
        try:
            query_job = self.bq_client.query(query, job_config=job_config)  # Make an API request.
            logger.info(f'Job {query_job.job_id} is currently in state {query_job.state}')
            if estimate:
                logger.info(f'Estimation: This query will process {query_job.total_bytes_processed} bytes ({query_job.total_bytes_processed/pow(1024,3)} GB).')
            else:
                query_job.result() # Wait for the job to complete.

        except Exception as err:
            logger.error(f'run_query - Unknown Error has occurred {err}.')
            sys.exit(1)

