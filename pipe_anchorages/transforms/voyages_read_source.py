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
        AND date(end_timestamp) <= "{end:%Y-%m-%d}"
        AND confidence >= 2

"""
    # ORDER BY end_timestamp, vessel_id
    # LIMIT 1000

class ReadSource(beam.PTransform):
    def __init__(self, source_table, first_table_date, cloud):
        self.first_table_date = dt.datetime.strptime(first_table_date,'%Y-%m-%d')
        self.source_table = source_table
        cloud_to_labels = lambda ll: {x.split('=')[0]:x.split('=')[1] for x in ll}
        self.labels = cloud_to_labels(cloud.labels)

    def expand(self, pcoll):
        return (
            [
                pcoll
                | f"Read_visits_{i}" >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True, bigquery_job_labels=self.labels)
                for i, query in enumerate(self.read_source())
            ]
            | beam.Flatten()
        )
        # return (
        #     pcoll
        #     | self.read_source()
        # )

    def read_source(self):
        start = self.first_table_date
        end = dt.datetime.now()
        while start <= end:
            next_start = start + dt.timedelta(days=365)
            min_end = min(next_start - dt.timedelta(days=1), end)

            yield SOURCE_QUERY_TEMPLATE.format(
                source_table=self.source_table,
                start=start,
                end=min_end,
            )
            start = next_start
    # def read_source(self):
    #     return beam.io.ReadFromBigQuery(query=SOURCE_QUERY_TEMPLATE.format(
    #         source_table=self.source_table,
    #         start=dt.datetime(2012,1,1),
    #         end=dt.datetime(2012,1,31),
    #     ), use_standard_sql=True, bigquery_job_labels=self.labels)

