import apache_beam as beam

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
        date(end_timestamp) >= '2012-01-01'
        AND confidence >= 2

    order by end_timestamp, vessel_id
    LIMIT 1000
"""
        # AND date(end_timestamp) < '2012-02-01'
    # order by end_timestamp, vessel_id
    # LIMIT 1000

class ReadSource(beam.PTransform):
    def __init__(self, source_table):
        self.source_table = source_table

    def expand(self, pcoll):
        return (
            pcoll
            | self.read_source()
        )

    def read_source(self):
        query = SOURCE_QUERY_TEMPLATE.format(
            source_table=self.source_table,
        )
        return beam.io.ReadFromBigQuery(
            query = query,
            use_standard_sql=True,
        )
