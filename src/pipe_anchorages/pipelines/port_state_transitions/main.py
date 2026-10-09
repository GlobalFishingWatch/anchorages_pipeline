import dataclasses
import datetime
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable

import apache_beam as beam

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.beam.pipeline.hooks import create_table_hook, delete_events_hook
from gfw.common.beam.transforms import ReadFromBigQuery, WriteToBigQueryWrapper
from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.query import Query

from pipe_anchorages import common as cmn
from pipe_anchorages.hooks import update_table_metadata_hook
from pipe_anchorages.pipelines.port_state_transitions.config import PortStateTransitionsConfig
from pipe_anchorages.pipelines.port_state_transitions.table_config import (
    PortStateTransitionsTableConfig,
    PortStateTransitionsTableDescription,
)
from pipe_anchorages.pipelines.port_state_transitions.transforms.core import (
    FindPortStateTransitions,
)
from pipe_anchorages.version import __version__


@dataclass
class MessagesBySegmentQuery(Query):
    """Position messages in [start_date, end_date), one row per position, keyed by segment.

    Optionally limited to the vessels `ssvid_filter` (a subquery or a list of ssvids) returns.
    Its fields are the template's variables (see template_vars).
    """

    source_messages: str
    start_date: datetime.date
    end_date: datetime.date
    ssvid_filter: str = None

    template_filename = "messages_by_segment.sql.j2"

    @property
    def template_vars(self) -> dict:
        return dataclasses.asdict(self)


@dataclass
class NamedAnchoragesQuery(Query):
    """Named anchorages, one row per anchorage.

    Its fields are the template's variables (see template_vars).
    """

    source_named_anchorages: str

    template_filename = "named_anchorages.sql.j2"

    @property
    def template_vars(self) -> dict:
        return dataclasses.asdict(self)


def read_ssvid_filter(ssvid_filter: str) -> str:
    """Returns the ssvid filter, read from a file if it's a path prefixed by @."""
    if ssvid_filter is None or not ssvid_filter.startswith("@"):
        return ssvid_filter

    with open(ssvid_filter[1:]) as f:
        return f.read()


def run(
    config: SimpleNamespace,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict = None,
    read_from_bigquery_factory: Callable = None,
    write_to_bigquery_factory: Callable = None,
    bq_client_factory: Callable = None,
    **kwargs: Any,
) -> None:
    config = PortStateTransitionsConfig.from_namespace(config)

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

    # Ensure that S2 Cell sizes are large enough that we don't miss ports.
    anchorage_visit_max_distance = max(
        config.anchorage_entry_dist_km, config.anchorage_exit_dist_km
    )
    assert anchorage_visit_max_distance * cmn.VISIT_SAFETY_FACTOR < 2 * cmn.approx_visit_cell_size

    table_config = PortStateTransitionsTableConfig(
        table_id=config.bq_out_port_state_transitions,
        description=PortStateTransitionsTableDescription(
            version=__version__,
            relevant_params={
                "bq_in_messages": config.bq_in_messages,
                "bq_in_named_anchorages": config.bq_in_named_anchorages,
                "start_date": config.start_date,
                "end_date": config.end_date,
                "ssvid_filter": config.ssvid_filter,
                "anchorage_entry_dist_km": config.anchorage_entry_dist_km,
                "anchorage_exit_dist_km": config.anchorage_exit_dist_km,
                "stopping_speed_knots": config.stopping_speed_knots,
                "starting_speed_knots": config.starting_speed_knots,
                "min_anchorage_gap_minutes": config.min_anchorage_gap_minutes,
            },
        ),
    )

    dag = LinearDag(
        sources=[
            ReadFromBigQuery.from_query(
                MessagesBySegmentQuery(
                    source_messages=config.bq_in_messages,
                    start_date=config.start_date,
                    end_date=config.end_date,
                    ssvid_filter=read_ssvid_filter(config.ssvid_filter),
                ).with_env(config.jinja_env),
                label="ReadMessagesBySegment",
                read_from_bigquery_factory=read_from_bigquery_factory,
                read_from_bigquery_kwargs={"bigquery_job_labels": config.labels},
            ),
        ],
        core=FindPortStateTransitions(
            anchorage_entry_dist_km=config.anchorage_entry_dist_km,
            anchorage_exit_dist_km=config.anchorage_exit_dist_km,
            stopping_speed_knots=config.stopping_speed_knots,
            starting_speed_knots=config.starting_speed_knots,
            min_anchorage_gap_minutes=config.min_anchorage_gap_minutes,
            start_date=config.start_date,
            end_date=config.end_date,
        ),
        side_inputs=ReadFromBigQuery.from_query(
            NamedAnchoragesQuery(
                source_named_anchorages=config.bq_in_named_anchorages,
            ).with_env(config.jinja_env),
            label="ReadNamedAnchorages",
            read_from_bigquery_factory=read_from_bigquery_factory,
            read_from_bigquery_kwargs={"bigquery_job_labels": config.labels},
        ),
        sinks=(
            WriteToBigQueryWrapper(
                table=table_config.table_id,
                schema=table_config.schema,
                write_to_bigquery_factory=write_to_bigquery_factory,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            ),
        ),
    )

    pipeline = Pipeline(
        dag=dag,
        pre_hooks=[
            create_table_hook(table_config, mock=config.mock_bq_clients),
            delete_events_hook(
                table_config,
                start_date=config.start_date,
                end_date=config.end_date,
                mock=config.mock_bq_clients,
            ),
        ],
        post_hooks=[update_table_metadata_hook(table_config, config.labels, bq_client_factory)],
        unparsed_args=config.unknown_unparsed_args,
        labels=config.labels,
        **config.unknown_parsed_args,
        **kwargs,
    )

    pipeline.run()
