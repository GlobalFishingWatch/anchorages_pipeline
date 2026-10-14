import dataclasses
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable

import apache_beam as beam

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.beam.pipeline.hooks import create_table_hook
from gfw.common.beam.transforms import ReadFromBigQuery, WriteToBigQueryWrapper
from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.query import Query

from pipe_anchorages.hooks import update_table_metadata_hook
from pipe_anchorages.pipelines.named_anchorages.config import NamedAnchoragesConfig
from pipe_anchorages.pipelines.named_anchorages.table_config import (
    NamedAnchoragesTableConfig,
    NamedAnchoragesTableDescription,
)
from pipe_anchorages.pipelines.named_anchorages.transforms.core import NameAnchorages
from pipe_anchorages.version import __version__


@dataclass
class AnchorageLocationsQuery(Query):
    """Anchorage locations, one row per anchorage.

    Its fields are the template's variables (see template_vars).
    """

    source_anchorage_locations: str

    template_filename = "anchorage_locations.sql.j2"

    @property
    def template_vars(self) -> dict:
        return dataclasses.asdict(self)


def run(
    config: SimpleNamespace,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict = None,
    read_from_bigquery_factory: Callable = None,
    write_to_bigquery_factory: Callable = None,
    bq_client_factory: Callable = None,
    **kwargs: Any,
) -> None:
    config = NamedAnchoragesConfig.from_namespace(config)

    if read_from_bigquery_factory is None:
        read_from_bigquery_factory = ReadFromBigQuery.get_client_factory(
            mocked=config.mock_bq_clients
        )
    if write_to_bigquery_factory is None:
        write_to_bigquery_factory = WriteToBigQueryWrapper.get_client_factory(
            mocked=config.mock_bq_clients
        )
    if bq_client_factory is None:
        bq_client_factory = BigQueryHelper.get_client_factory(mocked=config.mock_bq_clients)

    table_config = NamedAnchoragesTableConfig(
        table_id=config.bq_out_named_anchorages,
        description=NamedAnchoragesTableDescription(
            version=__version__,
            relevant_params={
                "bq_in_anchorage_locations": config.bq_in_anchorage_locations,
                "shapefile": config.shapefile,
                "label_distance_km": config.label_distance_km,
                "sublabel_distance_km": config.sublabel_distance_km,
                "anchorage_overrides": config.anchorage_overrides,
                "reference_ports": list(config.reference_ports),
                "reference_places": list(config.reference_places),
            },
        ),
    )

    dag = LinearDag(
        sources=[
            ReadFromBigQuery.from_query(
                AnchorageLocationsQuery(
                    source_anchorage_locations=config.bq_in_anchorage_locations,
                ).with_env(config.jinja_env),
                label="ReadAnchorageLocations",
                read_from_bigquery_factory=read_from_bigquery_factory,
                read_from_bigquery_kwargs={"bigquery_job_labels": config.labels},
            ),
        ],
        core=NameAnchorages(
            shapefile=config.shapefile,
            label_distance_km=config.label_distance_km,
            sublabel_distance_km=config.sublabel_distance_km,
            anchorage_overrides=config.anchorage_overrides,
            reference_ports=config.reference_ports,
            reference_places=config.reference_places,
        ),
        sinks=(
            WriteToBigQueryWrapper(
                table=table_config.table_id,
                schema=table_config.schema,
                write_to_bigquery_factory=write_to_bigquery_factory,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            ),
        ),
    )

    pipeline = Pipeline(
        dag=dag,
        pre_hooks=[create_table_hook(table_config, mock=config.mock_bq_clients)],
        post_hooks=[update_table_metadata_hook(table_config, config.labels, bq_client_factory)],
        unparsed_args=config.unknown_unparsed_args,
        labels=config.labels,
        **config.unknown_parsed_args,
        **kwargs,
    )

    pipeline.run()
