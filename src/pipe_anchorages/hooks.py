"""Pipeline hooks for the Beam pipelines' output tables.

Writing with WRITE_TRUNCATE and CREATE_NEVER, a pipeline creates its table, with its
partitioning and clustering, in a pre-hook (gfw-common's create_table_hook), and sets its
description and labels in a post-hook (update_table_metadata_hook), once the data is written.
Passing the description to the load job instead (destinationTableProperties) only works on new
tables: on an existing table, BigQuery fails the job if the description differs from the current
one.
"""
import logging
from typing import Callable

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.bigquery.table_config import TableConfig

logger = logging.getLogger(__name__)


def update_table_metadata_hook(
    table_config: TableConfig, labels: dict, bq_client_factory: Callable
) -> Callable[[Pipeline], None]:
    """Returns a hook that sets the table's description and labels."""

    def _hook(p: Pipeline) -> None:
        logger.info(f"Updating the description and labels of {table_config.table_id}...")
        bq = BigQueryHelper(bq_client_factory, project=p.cloud_options.project)
        bq.update_table_metadata(
            table_config.table_id,
            description=table_config.description.render(),
            labels=labels,
        )

    return _hook
