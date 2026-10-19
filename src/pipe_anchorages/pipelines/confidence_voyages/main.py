import logging
from types import SimpleNamespace
from typing import Callable
from functools import cached_property

from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.query import Query

from pipe_anchorages.version import __version__
from pipe_anchorages.pipelines.confidence_voyages.config import ConfidenceVoyagesConfig
from pipe_anchorages.pipelines.confidence_voyages.table_config import (
    CONFIDENCE_MEANING,
    ConfidenceVoyagesTableConfig,
    ConfidenceVoyagesTableDescription,
)

logger = logging.getLogger(__name__)


class ConfidenceVoyagesQuery(Query):
    def __init__(self, config: ConfidenceVoyagesConfig) -> None:
        self.config = config

    @cached_property
    def template_filename(self) -> str:
        return "confidence_voyages.sql.j2"

    @cached_property
    def template_vars(self) -> dict:
        return {
            "port_visits_table": self.config.bq_in_port_visits,
            "min_confidence": self.config.min_confidence,
        }


def run(
    config: SimpleNamespace,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict = None,
    bq_client_factory: Callable = None,
) -> None:

    config = ConfidenceVoyagesConfig.from_namespace(config)

    if bq_client_factory is None:
        bq_client_factory = BigQueryHelper.get_client_factory(mocked=config.mock_bq_clients)

    query = ConfidenceVoyagesQuery(config)
    bq = BigQueryHelper(bq_client_factory, project=config.project)

    table_config = ConfidenceVoyagesTableConfig(
        table_id=config.bq_out_voyages,
        description=ConfidenceVoyagesTableDescription(
            version=__version__,
            relevant_params={
                "source_port_visits": config.bq_in_port_visits,
                "min_confidence": (
                    f"{config.min_confidence} ({CONFIDENCE_MEANING[config.min_confidence]})"
                ),
            },
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
