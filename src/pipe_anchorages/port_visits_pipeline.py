import datetime
import logging
import math

from types import SimpleNamespace
from typing import Any

from google.cloud import bigquery
import pytz

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import PipelineState
from gfw.common.beam.pipeline.base import Pipeline

from pipe_anchorages import common as cmn
from pipe_anchorages.core.namedtuples import _datetime_to_s
from pipe_anchorages.schema.port_visit import port_visit_schema
from pipe_anchorages.transforms.create_in_out_events import CreateInOutEvents
from pipe_anchorages.transforms.create_port_visits import CreatePortVisits
from pipe_anchorages.transforms.sink import VisitsSink
from pipe_anchorages.transforms.smart_thin_records import VisitLocationRecord
from pipe_anchorages.transforms.source import QuerySource
from pipe_anchorages.utils.bqtools import BigQueryHelper, DatePartitionedTable
from pipe_anchorages.utils.ver import get_pipe_ver


def create_queries(config, start_date, end_date):
    template = """
    SELECT vids.ssvid,
           vids.vessel_id,
           vids.seg_id,
           records.* except (timestamp, identifier),
           CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp
    FROM `{table}` records
    JOIN `{vid_table}` vids
    ON records.identifier = vids.seg_id
    WHERE DATE(timestamp) BETWEEN '{start}' AND '{end}'
     {condition}
    """

    if config.bad_segs is None:
        condition = ""
    else:
        condition = f"  AND seg_id NOT IN (SELECT seg_id FROM {config.bad_segs})"

    start_window = start_date
    shift = 1000
    while start_window <= end_date:
        end_window = min(start_window + datetime.timedelta(days=shift), end_date)
        yield template.format(
            table=config.bq_in_port_state_transitions,
            vid_table=config.bq_in_segment_info,
            condition=condition,
            start=start_window,
            end=end_window,
        )
        start_window = end_window + datetime.timedelta(days=1)


def from_msg(x):
    x_new = x.copy()
    x_new["timestamp"] = datetime.datetime.fromtimestamp(x_new["timestamp"], datetime.UTC)
    ssvid = x_new.pop("ssvid")
    seg_id = x_new.pop("seg_id")
    vessel_id = x_new.pop("vessel_id")
    ident = (ssvid, vessel_id, seg_id)
    loc = cmn.LatLon(x_new.pop("lat"), x_new.pop("lon"))
    port_dist = x_new.pop("port_dist")
    if port_dist is None:
        port_dist = math.inf
    return vessel_id, VisitLocationRecord(
        identifier=ident, location=loc, port_dist=port_dist, **x_new
    )


def event_to_msg(x):
    x = x._asdict()
    x["timestamp"] = _datetime_to_s(x["timestamp"])
    x.pop("vessel_id")
    x.pop("last_timestamp")
    x.pop("ssvid")
    return x


def visit_to_msg(x):
    x = x._asdict()
    x["events"] = [event_to_msg(y) for y in x["events"]]
    x["start_timestamp"] = _datetime_to_s(x["start_timestamp"])
    x["end_timestamp"] = _datetime_to_s(x["end_timestamp"])
    return x


def drop_new_fields(x):
    excluded_fields = {"ssvid", "duration_hrs", "confidence"}
    return {key: value for key, value in x.items() if key not in excluded_fields}


def strdate_to_utcdatetime(strdate):
    return datetime.datetime.strptime(strdate, "%Y-%m-%d").replace(tzinfo=pytz.utc)


def prepare_output_tables(config, cloud_options, start_date, end_date):
    output_table = DatePartitionedTable(
        table_id=config.bq_out_port_visits,
        description=f"""
Created by the anchorages_pipeline: {get_pipe_ver()}.
Creates the visits to port table.
* https://github.com/GlobalFishingWatch/anchorages_pipeline
* Sources: {config.bq_in_port_state_transitions}
* Vessel id to join identification: {config.bq_in_segment_info}
* Skip bad segments: {"Yes" if config.bad_segs else "No"}
* Segments more than this distance apart will not be joined when creating visits: {config.max_inter_seg_dist_nm}
* Date end: {end_date}
        """,  # noqa: E501
        schema=port_visit_schema["fields"],
        partitioning_field="end_timestamp",
    )

    bq_helper = BigQueryHelper(
        bq_client=bigquery.Client(
            project=cloud_options.project,
        ),
        labels=config.labels or {},
    )

    bq_helper.ensure_table_exists(output_table)
    bq_helper.update_table(output_table)


def run(config: SimpleNamespace, **kwargs: Any) -> int:
    pipeline = Pipeline(
        unparsed_args=config.unknown_unparsed_args,
        labels=config.labels or None,
        **config.unknown_parsed_args,
        **kwargs,
    )
    cloud_options = pipeline.cloud_options

    # Ensure that S2 Cell sizes are large enough that we don't miss ports.
    anchorage_visit_max_distance = max(
        config.anchorage_entry_dist_km, config.anchorage_exit_dist_km
    )
    assert anchorage_visit_max_distance * cmn.VISIT_SAFETY_FACTOR < 2 * cmn.approx_visit_cell_size

    p = pipeline.pipeline

    start_time = strdate_to_utcdatetime(config.start_date)
    end_time = strdate_to_utcdatetime(config.end_date)

    start_date = start_time.date()
    end_date = end_time.date()

    queries = create_queries(config, start_date, end_date)

    sources = [
        (p | f"ReadThinnedMessagesJoinedVesselId_{i}" >> QuerySource(query, cloud_options))
        for (i, query) in enumerate(queries)
    ]

    _ = (
        sources
        | beam.Flatten()
        | beam.Map(from_msg)
        | beam.GroupByKey()
        | CreateInOutEvents(
            anchorage_entry_dist=config.anchorage_entry_dist_km,
            anchorage_exit_dist=config.anchorage_exit_dist_km,
            stopped_begin_speed=config.stopping_speed_knots,
            stopped_end_speed=config.starting_speed_knots,
            min_gap_minutes=config.min_anchorage_gap_minutes,
            end_time=end_time,
        )
        | CreatePortVisits(config.max_inter_seg_dist_nm)
        | beam.Map(visit_to_msg)
        | VisitsSink(config.bq_out_port_visits)
    )

    prepare_output_tables(config, cloud_options, start_date, end_date)
    result = p.run()

    success_states = set(
        [
            PipelineState.DONE,
            PipelineState.RUNNING,
            PipelineState.UNKNOWN,
            PipelineState.PENDING,
        ]
    )

    runner = pipeline.pipeline_options.view_as(StandardOptions).runner
    if config.wait_for_job or runner == "DirectRunner":
        result.wait_until_finish()

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
