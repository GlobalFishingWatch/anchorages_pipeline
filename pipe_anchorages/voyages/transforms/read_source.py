import apache_beam as beam
import datetime as dt

# The pipeline is defined to work from the beginning of 2012
# and only confidence 2 and greater are of interest for voyages.
SOURCE_QUERY_TEMPLATE = """
    SELECT
        ssvid,
        vessel_id,
        visit_id,
        start_anchorage_id,
        start_timestamp,
        end_anchorage_id,
        end_timestamp,
        confidence
    FROM
      `{source_table}`
    WHERE
        date(end_timestamp) >= "{start:%Y-%m-%d}"
        AND confidence >= 2

"""


def cloud_options_to_labels(options):
    return dict([x.split("=") for x in options.labels])


class ReadSource(beam.PTransform):
    def __init__(self, source_table, first_table_date, cloud_options):
        self.first_table_date = dt.datetime.strptime(first_table_date, "%Y-%m-%d")
        self.source_table = source_table
        self.labels = cloud_options_to_labels(cloud_options)

    def expand(self, pcoll):
        return pcoll | self.read_source()

    def read_source(self):
        query = SOURCE_QUERY_TEMPLATE.format(
            source_table=self.source_table, start=self.first_table_date
        )
        return beam.io.ReadFromBigQuery(
            query=query, use_standard_sql=True, bigquery_job_labels=self.labels
        )
