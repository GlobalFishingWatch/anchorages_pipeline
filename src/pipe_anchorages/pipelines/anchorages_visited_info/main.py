import logging
from types import SimpleNamespace
from typing import Callable
from functools import cached_property

from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.query import Query

from pipe_anchorages.version import __version__
from pipe_anchorages.pipelines.anchorages_visited_info.config import AnchoragesVisitedInfoConfig
from pipe_anchorages.pipelines.anchorages_visited_info.table_config import (
    AnchoragesVisitedInfoTableConfig,
    AnchoragesVisitedInfoTableDescription
)

logger = logging.getLogger(__name__)


class AnchoragesVisitedInfoQuery(Query):
    def __init__(self, config: AnchoragesVisitedInfoConfig) -> None:
        self.config = config

    @cached_property
    def template_filename(self) -> str:
        return "anchorages_visited_info.sql.j2"

    @cached_property
    def template_vars(self) -> dict:
        return {
            "source_loitering": self.config.bq_input_loitering,
            "source_encounters": self.config.bq_input_encounters,
            "source_ais_gaps": self.config.bq_input_ais_gaps,
            "source_named_anchorages": self.config.bq_input_named_anchorages,
        }


def run(
    config: SimpleNamespace,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict = None,
    bq_client_factory: Callable = None,
) -> None:

    config = AnchoragesVisitedInfoConfig.from_namespace(config)

    if bq_client_factory is None:
        bq_client_factory = BigQueryHelper.get_client_factory(config.mock_bq_clients)

    query = AnchoragesVisitedInfoQuery(config)
    bq = BigQueryHelper(bq_client_factory, dry_run=config.dry_run, project=config.project)

    table_config = AnchoragesVisitedInfoTableConfig(
        table_id=config.bq_output,
        description=AnchoragesVisitedInfoTableDescription(
            version=__version__,
            relevant_params={}
        ),
    )

    logger.info("Running query...")
    bq.run_query(
        query.render(),
        destination=table_config.table_id,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        partition_type=table_config.partition_type,
        partition_field=table_config.partition_field,
        clustering_fields=table_config.clustering_fields,
        schema=table_config.schema,
        description=table_config.description.render(),
        labels=config.labels,
    )
    logger.info("Done.")
    logger.info("You can check the results in:")
    logger.info(f"{table_config.table_id}")
