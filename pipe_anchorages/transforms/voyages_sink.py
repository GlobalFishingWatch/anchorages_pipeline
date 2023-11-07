import apache_beam as beam
from google.cloud import bigquery
from pipe_anchorages.utils.ver import get_pipe_ver


"""
Writes the voyages
"""
class WriteSink(beam.PTransform):

    TABLE_SCHEMA = {
        "fields": [
            {
              "mode": "NULLABLE",
              "name": "ssvid",
              "type": "STRING",
              "description": "The Specific Source Vessel ID, in this case the MMSI."
            },
            {
              "mode": "NULLABLE",
              "name": "vessel_id",
              "type": "STRING",
              "description": "The unique vessel id. This table has one row per vessel_id."
            },
            {
              "mode": "NULLABLE",
              "name": "trip_start",
              "type": "TIMESTAMP",
              "description": "The moment when the trip started."
            },
            {
              "mode": "NULLABLE",
              "name": "trip_end",
              "type": "TIMESTAMP",
              "description": "The moment when the trip ended."
            },
            {
              "mode": "NULLABLE",
              "name": "trip_start_anchorage_id",
              "type": "STRING",
              "description": "The anchorage id where the trip started."
            },
            {
              "mode": "NULLABLE",
              "name": "trip_end_anchorage_id",
              "type": "STRING",
              "description": "The anchorage id where the trip ended."
            },
            {
              "mode": "NULLABLE",
              "name": "trip_start_visit_id",
              "type": "STRING",
              "description": "The visit id from the trip started."
            },
            {
              "mode": "NULLABLE",
              "name": "trip_end_visit_id",
              "type": "STRING",
              "description": "The visit id from the trip ended."
            },
            {
              "mode": "NULLABLE",
              "name": "trip_start_confidence",
              "type": "INTEGER",
              "description": "The confidence of the visit where the trip started."
            },
            {
              "mode": "NULLABLE",
              "name": "trip_end_confidence",
              "type": "INTEGER",
              "description": "The confidence of the visit where the trip ended."
            },
            {
              "mode": "NULLABLE",
              "name": "trip_id",
              "type": "STRING",
              "description": "The confidence of the visit where the trip ended."
            },
        ]
    }

    confidence_meaning = {
        2: "only stop and/or gap; no entry or exit",
        3: "port entry or exit with stop and/or gap",
        4: "port entry and exit with stop and/or gap",
    }

    def __init__(self, options, cloud_options):
        self.sink_table = options.output_table
        self.options = options
        self.cloud = cloud_options
        self.ver = get_pipe_ver()

    def expand(self, pcoll):
        return [ (
            pcoll
            | (f'filter_{c}' >> self.filter_confidence(c)
            | f'clear_{c}' >> beam.Map(self.clear)
            | f'write_{c}' >> self.write_sink(c))
        ) for c in [2,3,4]]

    def filter_confidence(self, confidence):
        return beam.Filter(lambda x: x['trip_confidence'] == confidence)

    def clear(self, v):
        return {
            'ssvid': v['ssvid'],
            'vessel_id': v['vessel_id'],
            'trip_start': v['trip_start'],
            'trip_end': v['trip_end'],
            'trip_start_anchorage_id': v['trip_start_anchorage_id'],
            'trip_end_anchorage_id': v['trip_end_anchorage_id'],
            'trip_start_visit_id': v['trip_start_visit_id'],
            'trip_end_visit_id': v['trip_end_visit_id'],
            'trip_start_confidence': v['trip_start_confidence'],
            'trip_end_confidence': v['trip_end_confidence'],
            'trip_id': v['trip_id']
        }

    def get_description(self, min_confidence):
        return f"""
Created by pipe-anchorages: {self.ver}
* Create voyages filter per minimal confidence.
* https://github.com/GlobalFishingWatch/anchorages_pipeline
* Source: {self.options.source_table}
* Minimal confidence: {min_confidence} meaning: {WriteSink.confidence_meaning[min_confidence]}"""

    def write_sink(self, confidence):
        return beam.io.WriteToBigQuery(
            f"{self.sink_table}{confidence}",
            schema=WriteSink.TABLE_SCHEMA,
            additional_bq_parameters={
                "timePartitioning": {
                    "type": "MONTH",
                    "field": 'trip_start',
                    "requirePartitionFilter": True
                },
                "clustering": {
                    "fields": ["trip_start", "ssvid", "vessel_id", "trip_id"]
                },
                "destinationTableProperties": {
                    "description": self.get_description(confidence),
                },
            },
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
        # return beam.io.WriteToText(f'voyages_c{confidence}')

    def _get_table(self, bqclient:bigquery.Client, confidence:int):
        dataset_id, table_name = self.sink_table.split('.')
        dataset_ref = bigquery.DatasetReference(self.cloud.project, dataset_id)
        table_ref = dataset_ref.table(f'{table_name}{confidence}')
        return bqclient.get_table(table_ref)  # API request

    def update_labels(self):
        # return None
        bqclient = bigquery.Client(project=self.cloud.project)
        for c in [2,3,4]:
            table = self._get_table(bqclient,c)
            cloud_to_labels = lambda ll: {x.split('=')[0]:x.split('=')[1] for x in ll}
            table.labels = cloud_to_labels(self.cloud.labels)
            bqclient.update_table(table, ["labels"])  # API request

