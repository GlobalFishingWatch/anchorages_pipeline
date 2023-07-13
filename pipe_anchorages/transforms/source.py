
from apache_beam import PTransform
from apache_beam import io
from pipe_anchorages.transforms.sink import cloud_to_labels


class QuerySource(PTransform):

    def __init__(self, query, cloud_options, use_standard_sql=True):
        self.query = query
        self.use_standard_sql = use_standard_sql
        self.labels = cloud_to_labels(cloud_options.labels)

    def expand(self, xs):
        return (
            xs
            | io.ReadFromBigQuery(
                query=self.query,
                use_standard_sql=self.use_standard_sql,
                bigquery_job_labels = self.labels
            )
        )
