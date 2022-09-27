from __future__ import absolute_import, division, print_function

import datetime
import logging

import apache_beam as beam
import pytz
from apache_beam import Map, io
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import PipelineState

from . import common as cmn
from .objects.namedtuples import _datetime_to_s
from .options.port_visits_options import PortVisitsOptions
from .records import VesselLocationRecord
from .schema.port_visit import build as build_visit_schema
from .transforms.create_in_out_events import CreateInOutEvents
from .transforms.create_port_visits import CreatePortVisits
from .transforms.create_tagged_anchorages import CreateTaggedAnchorages
from .transforms.source import QuerySource

# TODO: move to transform and import to both port_events (smart thing) and port_visits
# Maybe integrate into CreateTaggedAnchorages

# TODO: records should just use seg_id and any track stuff should happen here
# TODO: on the joining with vessel_id (becomes track_id?)
# TODO: add in bad_segs table
def create_queries(args, end_date):
    template = """
    SELECT vids.ssvid, vids.vessel_id, vids.seg_id, records.* except (timestamp, identifier),
            CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp
    FROM `{table}*` records
    JOIN `{vid_table}` vids
    ON records.identifier = vids.seg_id
    WHERE records._table_suffix <= '{end:%Y%m%d}'
     {condition}
    """
    if args.bad_segs is None:
        condition = ""
    else:
        condition = f"  AND seg_id NOT IN (SELECT seg_id FROM {args.bad_segs})"
    yield template.format(
        table=args.thinned_message_table,
        vid_table=args.vessel_id_table,
        condition=condition,
        end=end_date,
    )

    # start_window = start_date
    # shift = 1000
    # while start_window <= end_date:
    #     end_window = min(start_window + datetime.timedelta(days=shift), end_date)
    #     query = template.format(
    #         table=args.thinned_message_table,
    #         vid_table=args.vessel_id_table,
    #         condition=condition,
    #         start=start_window,
    #         end=end_window,
    #     )
    #     yield query
    #     start_window = end_window + datetime.timedelta(days=1)


anchorage_query = (
    "SELECT lat as anchor_lat, lon as anchor_lon, s2id as anchor_id, label FROM `{}`"
)


def from_msg(x):
    x = x.copy()
    x["timestamp"] = datetime.datetime.utcfromtimestamp(x["timestamp"]).replace(
        tzinfo=pytz.utc
    )
    ssvid = x.pop("ssvid")
    seg_id = x.pop("seg_id")
    vessel_id = x.pop("vessel_id")
    ident = (ssvid, vessel_id, seg_id)
    loc = cmn.LatLon(x.pop("lat"), x.pop("lon"))
    return vessel_id, VesselLocationRecord(identifier=ident, location=loc, **x)


def event_to_msg(x):
    x = x._asdict()
    x["timestamp"] = _datetime_to_s(x["timestamp"])
    x.pop("vessel_id")
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


def run(options):

    visit_args = options.view_as(PortVisitsOptions)

    config = cmn.load_config(visit_args.config)

    p = beam.Pipeline(options=options)

    end_date = (
        datetime.datetime.strptime(visit_args.end_date, "%Y-%m-%d")
        .replace(tzinfo=pytz.utc)
        .date()
    )

    # TODO: update options, moving stuff to here

    anchorages = (
        p
        # TODO: why not just use BigQuerySource?
        | "ReadAnchorages"
        >> QuerySource(
            anchorage_query.format(visit_args.anchorage_table), use_standard_sql=True
        )
        | CreateTaggedAnchorages()
    )

    sink = io.WriteToBigQuery(
        visit_args.output_table,
        schema=build_visit_schema(),
        write_disposition=io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=io.BigQueryDisposition.CREATE_IF_NEEDED,
        additional_bq_parameters={
            "timePartitioning": {"type": "DAY", "field": "end_timestamp"},
            "clustering": {
                "fields": ["start_timestamp", "confidence", "ssvid", "vessel_id"]
            },
        },
    )

    queries = create_queries(visit_args, end_date)

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

    (
        sources
        | beam.Flatten()
        | beam.Map(from_msg)
        | beam.GroupByKey()
        | CreateInOutEvents(
            anchorages=anchorages,
            anchorage_entry_dist=config["anchorage_entry_distance_km"],
            anchorage_exit_dist=config["anchorage_exit_distance_km"],
            stopped_begin_speed=config["stopped_begin_speed_knots"],
            stopped_end_speed=config["stopped_end_speed_knots"],
            min_gap_minutes=config["minimum_port_gap_duration_minutes"],
            end_date=end_date,
        )
        | CreatePortVisits(visit_args.max_inter_seg_dist_nm)
        | Map(visit_to_msg)
        | sink
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
        visit_args.wait_for_job
        or options.view_as(StandardOptions).runner == "DirectRunner"
    ):
        result.wait_until_finish()

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
