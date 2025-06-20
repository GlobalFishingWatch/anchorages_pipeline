import datetime
import logging

from google.cloud import bigquery

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, StandardOptions
from apache_beam.runners import PipelineState
from pipe_anchorages import common as cmn
from pipe_anchorages.options.thin_port_messages_options import ThinPortMessagesOptions
from pipe_anchorages.schema.message_schema import message_schema
from pipe_anchorages.transforms.create_tagged_anchorages import CreateTaggedAnchorages
from pipe_anchorages.transforms.sink import MessageSink
from pipe_anchorages.transforms.smart_thin_records import SmartThinRecords
from pipe_anchorages.transforms.source import QuerySource
from pipe_anchorages.utils.bqtools import BigQueryHelper, DateShardedTable
from pipe_anchorages.utils.tools import list_of_days
from pipe_anchorages.utils.ver import get_pipe_ver


def create_queries(args, start_date, end_date):
    template = """
    SELECT seg_id as ident, ssvid, lat, lon, speed,
            CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp
    FROM `{table}`
    WHERE date(timestamp) BETWEEN '{start:%Y-%m-%d}' AND '{end:%Y-%m-%d}'
      {filter_text}
    """
    start_window = start_date
    shift = 1000
    if args.ssvid_filter is None:
        filter_text = ""
    else:
        filter_core = args.ssvid_filter
        if filter_core.startswith("@"):
            with open(args.ssvid_filter[1:]) as f:
                filter_core = f.read()
        filter_text = f"AND ssvid in ({filter_core})"

    while start_window <= end_date:
        end_window = min(start_window + datetime.timedelta(days=shift), end_date)
        query = template.format(
            table=args.input_table,
            filter_text=filter_text,
            start=start_window,
            end=end_window,
        )
        yield query
        start_window = end_window + datetime.timedelta(days=1)


def anchorage_query(args):
    return f"""
    SELECT lat as anchor_lat, lon as anchor_lon, s2id as anchor_id, label
    FROM `{args.anchorage_table}`
    """


def prepare_output_tables(pipe_options, cloud_options, start_date, end_date):
    output_table = DateShardedTable(
        table_id_prefix=pipe_options.output_table,
        description=f"""
Created by the anchorages_pipeline: {get_pipe_ver()}.
* Creates raw thinned messages in out port events.
* https://github.com/GlobalFishingWatch/anchorages_pipeline
* Sources: {pipe_options.input_table}
* Anchorage table: {pipe_options.anchorage_table}
* Date: {start_date}, {end_date}
        """,
        schema=message_schema["fields"],
    )

    bq_helper = BigQueryHelper(
        bq_client=bigquery.Client(
            project=cloud_options.project,
        ),
        labels=dict([entry.split("=") for entry in cloud_options.labels]),
    )

    # list_of_days doesn't include the end date. However, in daily mode,
    # start and end date are the same day.
    for date in list_of_days(start_date, end_date + datetime.timedelta(days=1)):
        shard = output_table.build_shard(date)
        bq_helper.ensure_table_exists(shard)
        bq_helper.update_table(shard)


def run(options):
    known_args = options.view_as(ThinPortMessagesOptions)
    cloud_options = options.view_as(GoogleCloudOptions)

    start_date = datetime.datetime.strptime(known_args.start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(known_args.end_date, "%Y-%m-%d").date()

    p = beam.Pipeline(options=options)

    config = cmn.load_config(known_args.config)

    queries = create_queries(known_args, start_date, end_date)

    sources = [
        (p | f"Read_{i}" >> QuerySource(query, cloud_options)) for (i, query) in enumerate(queries)
    ]

    tagged_records = (
        sources
        | beam.Flatten()
        | cmn.CreateVesselRecords(destination=None)
        | cmn.CreateTaggedRecordsByDay()
    )

    anchorages = (
        p
        | "ReadAnchorages" >> QuerySource(anchorage_query(known_args), cloud_options)
        | CreateTaggedAnchorages()
    )

    _ = (
        tagged_records
        | "thinRecords" >> SmartThinRecords(
            anchorages=anchorages,
            anchorage_entry_dist=config["anchorage_entry_distance_km"],
            anchorage_exit_dist=config["anchorage_exit_distance_km"],
            stopped_begin_speed=config["stopped_begin_speed_knots"],
            stopped_end_speed=config["stopped_end_speed_knots"],
            min_gap_minutes=config["minimum_port_gap_duration_minutes"],
            start_date=start_date,
            end_date=end_date,
        )
        | "writeThinnedRecords" >> MessageSink(known_args.output_table)
    )

    prepare_output_tables(known_args, cloud_options, start_date, end_date)
    result = p.run()

    success_states = set(
        [
            PipelineState.DONE,
            PipelineState.RUNNING,
            PipelineState.UNKNOWN,
            PipelineState.PENDING,
        ]
    )

    if known_args.wait_for_job or options.view_as(StandardOptions).runner == "DirectRunner":
        result.wait_until_finish()

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
