from __future__ import absolute_import, division, print_function

import datetime
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import (GoogleCloudOptions,
                                                  StandardOptions)
from apache_beam.runners import PipelineState

from . import common as cmn
from .options.port_events_options import PortEventsOptions
from .transforms.create_tagged_anchorages import CreateTaggedAnchorages
from .transforms.sink import MessageSink
from .transforms.smart_thin_records import SmartThinRecords
from .transforms.source import QuerySource


def create_queries(args, start_date, end_date):
    template = """
    SELECT seg_id AS ident, ssvid, lat, lon, speed,
            CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp
    FROM `{table}*`
    WHERE _table_suffix BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}'
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


anchorage_query = (
    "SELECT lat as anchor_lat, lon as anchor_lon, s2id as anchor_id, label FROM `{}`"
)


def run(options):

    known_args = options.view_as(PortEventsOptions)
    cloud_options = options.view_as(GoogleCloudOptions)

    start_date = datetime.datetime.strptime(known_args.start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(known_args.end_date, "%Y-%m-%d").date()

    p = beam.Pipeline(options=options)

    config = cmn.load_config(known_args.config)

    queries = create_queries(known_args, start_date, end_date)

    sources = [
        (
            p
            | "Read_{}".format(i)
            >> beam.io.Read(
                beam.io.gcp.bigquery.BigQuerySource(query=x, use_standard_sql=True)
            )
        )
        for (i, x) in enumerate(queries)
    ]

    tagged_records = (
        sources
        | beam.Flatten()
        | cmn.CreateVesselRecords(destination=None)
        | cmn.CreateTaggedRecordsByDay()
    )

    anchorages = (
        p
        # TODO: why not just use BigQuerySource?
        | "ReadAnchorages"
        >> QuerySource(
            anchorage_query.format(known_args.anchorage_table), use_standard_sql=True
        )
        | CreateTaggedAnchorages()
    )

    (
        tagged_records
        | "thinRecords"
        >> SmartThinRecords(
            anchorages=anchorages,
            anchorage_entry_dist=config["anchorage_entry_distance_km"],
            anchorage_exit_dist=config["anchorage_exit_distance_km"],
            stopped_begin_speed=config["stopped_begin_speed_knots"],
            stopped_end_speed=config["stopped_end_speed_knots"],
            min_gap_minutes=config["minimum_port_gap_duration_minutes"],
            start_date=start_date,
            end_date=end_date,
        )
        | "writeThinnedRecords"
        >> MessageSink(
            table=known_args.output_table,
            temp_location=cloud_options.temp_location,
            project=cloud_options.project,
        )
    )

    result = p.run()

    success_states = set(
        [
            PipelineState.DONE,
            PipelineState.RUNNING,
            PipelineState.UNKNOWN,
            PipelineState.PENDING,
        ]
    )

    if (
        known_args.wait_for_job
        or options.view_as(StandardOptions).runner == "DirectRunner"
    ):
        result.wait_until_finish()

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
