
from apache_beam import PTransform
from apache_beam import io

class QuerySource(PTransform):

    def __init__(self, query):
        self.query = query

    def expand(self, xs):
        return (
            xs
            | io.Read(io.gcp.bigquery.BigQuerySource(query=self.query))
        )
