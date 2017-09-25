
from apache_beam import PTransform
from apache_beam import io

class Source(PTransform):
    @staticmethod
    def read_query(path):
        with open(path, 'r') as query_file:
            return query_file.read()

    def __init__(self, query):
        self.query = query

    def expand(self, xs):
        return (
            xs
            | io.Read(io.gcp.bigquery.BigQuerySource(query=self.query))
        )

