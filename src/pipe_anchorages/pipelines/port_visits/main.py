import datetime
import logging
from datetime import date
from functools import cached_property
from types import SimpleNamespace
from typing import Any, Callable, Iterator

import apache_beam as beam
from apache_beam.runners import PipelineState

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.beam.transforms import ReadFromBigQuery, WriteToBigQueryWrapper
from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.query import Query

from pipe_anchorages import common as cmn
from pipe_anchorages.pipelines.port_visits.config import PortVisitsConfig
from pipe_anchorages.pipelines.port_visits.table_config import (
    PortVisitsTableConfig,
    PortVisitsTableDescription,
)
from pipe_anchorages.pipelines.port_visits.transforms.core import DetectPortVisits
from pipe_anchorages.version import __version__

logger = logging.getLogger(__name__)

# Days added to each query window's start date to get its (inclusive) end date.
QUERY_WINDOW_DAYS = 1000


class PortVisitsQuery(Query):
    """Encapsulates the port state transitions query, joined to each segment's vessel_id."""

    NAME = "port_visits"
    JINJA_TEMPLATE_FILENAME = "port_visits.sql.j2"

    def __init__(
        self,
        source_port_state_transitions: str,
        source_segment_info: str,
        start_date: date,
        end_date: date,
        bad_segs: str = None,
    ):
        self._source_port_state_transitions = source_port_state_transitions
        self._source_segment_info = source_segment_info
        self._start_date = start_date
        self._end_date = end_date
        self._bad_segs = bad_segs

    @cached_property
    def template_filename(self) -> str:
        return self.JINJA_TEMPLATE_FILENAME

    @cached_property
    def template_vars(self) -> dict:
        return {
            "source_port_state_transitions": self._source_port_state_transitions,
            "source_segment_info": self._source_segment_info,
            "start_date": self._start_date,
            "end_date": self._end_date,
            "bad_segs": self._bad_segs,
        }


def query_windows(start_date: date, end_date: date) -> Iterator[tuple[date, date]]:
    """Splits [start_date, end_date] into consecutive inclusive windows, one query each."""
    start_window = start_date
    while start_window <= end_date:
        end_window = min(start_window + datetime.timedelta(days=QUERY_WINDOW_DAYS), end_date)
        yield start_window, end_window
        start_window = end_window + datetime.timedelta(days=1)


def strdate_to_utcdatetime(strdate: str) -> datetime.datetime:
    return datetime.datetime.strptime(strdate, "%Y-%m-%d").replace(tzinfo=datetime.UTC)


def prepare_output_table_hook(
    table_config: PortVisitsTableConfig, labels: dict, bq_client_factory: Callable
) -> Callable[[Pipeline], None]:
    """Returns a pre-hook that creates the output table if needed, and sets its metadata.

    The sink writes with CREATE_NEVER, so the table (with its partitioning and clustering)
    must exist before the pipeline runs. An existing table gets the current description and
    labels.
    """

    def _hook(p: Pipeline) -> None:
        bq = BigQueryHelper(bq_client_factory, project=p.cloud_options.project)
        bq.create_table(**table_config.to_bigquery_params(), labels=labels, exists_ok=True)
        bq.update_table_metadata(
            table_config.table_id,
            description=table_config.description.render(),
            labels=labels,
        )

    return _hook


def run(
    config: SimpleNamespace,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict = None,
    read_from_bigquery_factory: Callable = None,
    write_to_bigquery_factory: Callable = None,
    bq_client_factory: Callable = None,
    **kwargs: Any,
) -> int:
    config = PortVisitsConfig.from_namespace(config, version=__version__)

    if read_from_bigquery_factory is None:
        read_from_bigquery_factory = ReadFromBigQuery.get_client_factory(
            mocked=bool(config.mock_bq_clients)
        )
    if write_to_bigquery_factory is None:
        write_to_bigquery_factory = WriteToBigQueryWrapper.get_client_factory(
            mocked=bool(config.mock_bq_clients)
        )
    if bq_client_factory is None:
        bq_client_factory = BigQueryHelper.get_client_factory(mocked=bool(config.mock_bq_clients))

    # Ensure that S2 Cell sizes are large enough that we don't miss ports.
    anchorage_visit_max_distance = max(
        config.anchorage_entry_dist_km, config.anchorage_exit_dist_km
    )
    assert anchorage_visit_max_distance * cmn.VISIT_SAFETY_FACTOR < 2 * cmn.approx_visit_cell_size

    start_time = strdate_to_utcdatetime(config.start_date)
    end_time = strdate_to_utcdatetime(config.end_date)

    table_config = PortVisitsTableConfig(
        table_id=config.bq_out_port_visits,
        description=PortVisitsTableDescription(
            version=__version__,
            relevant_params={
                "bq_in_port_state_transitions": config.bq_in_port_state_transitions,
                "bq_in_segment_info": config.bq_in_segment_info,
                "start_date": config.start_date,
                "end_date": config.end_date,
                "bad_segs": config.bad_segs,
                "max_inter_seg_dist_nm": config.max_inter_seg_dist_nm,
                "anchorage_entry_dist_km": config.anchorage_entry_dist_km,
                "anchorage_exit_dist_km": config.anchorage_exit_dist_km,
                "stopping_speed_knots": config.stopping_speed_knots,
                "starting_speed_knots": config.starting_speed_knots,
                "min_anchorage_gap_minutes": config.min_anchorage_gap_minutes,
            },
        ),
    )

    sources = [
        ReadFromBigQuery.from_query(
            PortVisitsQuery(
                source_port_state_transitions=config.bq_in_port_state_transitions,
                source_segment_info=config.bq_in_segment_info,
                start_date=start_window,
                end_date=end_window,
                bad_segs=config.bad_segs,
            ).with_env(config.jinja_env),
            label=f"ReadPortStateTransitions_{i}",
            read_from_bigquery_factory=read_from_bigquery_factory,
            read_from_bigquery_kwargs={"bigquery_job_labels": config.labels},
        )
        for i, (start_window, end_window) in enumerate(
            query_windows(start_time.date(), end_time.date())
        )
    ]

    dag = LinearDag(
        sources=sources,
        core=DetectPortVisits(
            anchorage_entry_dist_km=config.anchorage_entry_dist_km,
            anchorage_exit_dist_km=config.anchorage_exit_dist_km,
            stopping_speed_knots=config.stopping_speed_knots,
            starting_speed_knots=config.starting_speed_knots,
            min_anchorage_gap_minutes=config.min_anchorage_gap_minutes,
            max_inter_seg_dist_nm=config.max_inter_seg_dist_nm,
            end_time=end_time,
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
        name="pipe-anchorages",
        version=__version__,
        dag=dag,
        pre_hooks=[prepare_output_table_hook(table_config, config.labels, bq_client_factory)],
        unparsed_args=config.unknown_unparsed_args,
        labels=config.labels,
        **config.unknown_parsed_args,
        **kwargs,
    )

    result, _ = pipeline.run()

    success_states = {
        PipelineState.DONE,
        PipelineState.RUNNING,
        PipelineState.UNKNOWN,
        PipelineState.PENDING,
    }

    logger.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
