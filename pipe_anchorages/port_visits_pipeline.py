import datetime
import logging
import math

from google.cloud import bigquery
import pytz

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, StandardOptions
from apache_beam.runners import PipelineState
from pipe_anchorages import common as cmn
from pipe_anchorages.objects.namedtuples import _datetime_to_s
from pipe_anchorages.options.port_visits_options import PortVisitsOptions
from pipe_anchorages.schema.port_visit import port_visit_schema
from pipe_anchorages.transforms.create_in_out_events import CreateInOutEvents
from pipe_anchorages.transforms.create_port_visits import CreatePortVisits
from pipe_anchorages.transforms.sink import VisitsSink
from pipe_anchorages.transforms.smart_thin_records import VisitLocationRecord
from pipe_anchorages.transforms.source import QuerySource
from pipe_anchorages.utils.bqtools import BigQueryHelper, DatePartitionedTable
from pipe_anchorages.utils.ver import get_pipe_ver


def create_queries(args, start_date, end_date):
    template = """
    SELECT vids.ssvid,
           vids.vessel_id,
           vids.seg_id,
           records.* except (timestamp, identifier),
           CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp
    FROM `{table}*` records
    JOIN `{vid_table}` vids
    ON records.identifier = vids.seg_id
    WHERE records._table_suffix BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}'
     {condition}
    """

    if args.bad_segs is None:
        condition = ""
    else:
        condition = f"  AND seg_id NOT IN (SELECT seg_id FROM {args.bad_segs})"

    start_window = start_date
    shift = 1000
    while start_window <= end_date:
        end_window = min(start_window + datetime.timedelta(days=shift), end_date)
        yield template.format(
            table=args.thinned_message_table,
            vid_table=args.vessel_id_table,
            condition=condition,
            start=start_window,
            end=end_window,
        )
        start_window = end_window + datetime.timedelta(days=1)


def from_msg(x):
    x_new = x.copy()
    x_new["timestamp"] = datetime.datetime.utcfromtimestamp(x_new["timestamp"]).replace(
        tzinfo=pytz.utc
    )
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


def strdate_to_utcdate(strdate):
    return datetime.datetime.strptime(strdate, "%Y-%m-%d").replace(tzinfo=pytz.utc)


def prepare_output_tables(pipe_options, cloud_options, start_date, end_date):
    output_table = DatePartitionedTable(
        table_id=pipe_options.output_table,
        description=f"""
Created by the anchorages_pipeline: {get_pipe_ver()}.
Creates the visits to port table.
* https://github.com/GlobalFishingWatch/anchorages_pipeline
* Sources: {pipe_options.thinned_message_table}
* Vessel id to join identification: {pipe_options.vessel_id_table}
* Configuration file: {pipe_options.config}
* Skip bad segments: {"Yes" if pipe_options.bad_segs else "No"}
* Segments more than this distance apart will not be joined when creating visits: {pipe_options.max_inter_seg_dist_nm}
* Date end: {end_date}
        """,  # noqa: E501
        schema=port_visit_schema["fields"],
        partitioning_field="end_timestamp",
    )

    bq_helper = BigQueryHelper(
        bq_client=bigquery.Client(
            project=cloud_options.project,
        ),
        labels=dict([entry.split("=") for entry in cloud_options.labels]),
    )

    bq_helper.ensure_table_exists(output_table)
    bq_helper.update_table(output_table)


def run(options):
    visit_args = options.view_as(PortVisitsOptions)
    cloud_args = options.view_as(GoogleCloudOptions)

    config = cmn.load_config(visit_args.config)

    pipeline = beam.Pipeline(options=options)

    start_date = strdate_to_utcdate(visit_args.start_date)
    end_date = strdate_to_utcdate(visit_args.end_date)

    queries = create_queries(visit_args, start_date, end_date)

    sources = [
        (pipeline | f"ReadThinnedMessagesJoinedVesselId_{i}" >> QuerySource(query, cloud_args))
        for (i, query) in enumerate(queries)
    ]

    _ = (
        sources
        | beam.Flatten()
        | beam.Map(from_msg)
        | beam.GroupByKey()
        | CreateInOutEvents(
            anchorage_entry_dist=config["anchorage_entry_distance_km"],
            anchorage_exit_dist=config["anchorage_exit_distance_km"],
            stopped_begin_speed=config["stopped_begin_speed_knots"],
            stopped_end_speed=config["stopped_end_speed_knots"],
            min_gap_minutes=config["minimum_port_gap_duration_minutes"],
            end_date=end_date,
        )
        | CreatePortVisits(visit_args.max_inter_seg_dist_nm)
        | beam.Map(visit_to_msg)
        | VisitsSink(visit_args.output_table)
    )

    prepare_output_tables(visit_args, cloud_args, start_date, end_date)
    result = pipeline.run()

    success_states = set(
        [
            PipelineState.DONE,
            PipelineState.RUNNING,
            PipelineState.UNKNOWN,
            PipelineState.PENDING,
        ]
    )

    if visit_args.wait_for_job or options.view_as(StandardOptions).runner == "DirectRunner":
        result.wait_until_finish()

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
