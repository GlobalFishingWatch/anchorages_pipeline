
from apache_beam import PTransform
from apache_beam import io

class QuerySource(PTransform):

    def __init__(self, query, use_standard_sql=False):
        self.query = query
        self.use_standard_sql = use_standard_sql

    def expand(self, xs):
        return (
            xs
            | io.ReadFromBigQuery(query=self.query, use_standard_sql=self.use_standard_sql)
        )
