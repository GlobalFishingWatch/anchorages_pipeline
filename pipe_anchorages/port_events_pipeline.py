from __future__ import absolute_import, print_function, division

import datetime
import logging
import pytz

import apache_beam as beam
from apache_beam import Filter
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import PipelineState

from . import common as cmn
from .transforms.source import QuerySource
from .transforms.create_tagged_anchorages import CreateTaggedAnchorages
from .transforms.create_in_out_events import CreateInOutEvents
from .transforms.sink import EventSink
from .options.port_events_options import PortEventsOptions


def create_queries(args):
    template = """
    with 

    track_id as (
      select track_id, seg_id as aug_seg_id
      from `world-fishing-827.gfw_tasks_1143_new_segmenter.tracks_v20200229d_20191231`
      cross join unnest (seg_ids) as seg_id
    ),

    base as (
        select *,
               concat(seg_id, '-', format_date('%F', date(timestamp))) aug_seg_id
        from `world-fishing-827.pipe_production_v20200203.messages_segmented_*`
        where _table_suffix between '{start:%Y%m%d}' and '{end:%Y%m%d}' 
    ),

    source as (
        select base.*, track_id
        from base
        join (select * from track_id)
        using (aug_seg_id)
    )


    select track_id as ident, lat, lon, timestamp, speed from 
       source
    """
    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d') 
    start_window = start_date - datetime.timedelta(days=args.start_padding)
    end_date= datetime.datetime.strptime(args.end_date, '%Y-%m-%d') 
    shift = 1000 - args.start_padding
    while start_window <= end_date:
        end_window = min(start_window + datetime.timedelta(days=shift), end_date)
        query = template.format(table=args.input_table, start=start_window, end=end_window)
        if args.fast_test:
            query += 'LIMIT 100000'
        yield query
        start_window = end_window + datetime.timedelta(days=1)


anchorage_query = 'SELECT lat anchor_lat, lon anchor_lon, s2id as anchor_id, label FROM [{}]'


def run(options):

    known_args = options.view_as(PortEventsOptions)
    cloud_options = options.view_as(GoogleCloudOptions)

    start_date = datetime.datetime.strptime(known_args.start_date, '%Y-%m-%d').replace(tzinfo=pytz.utc) 
    end_date = datetime.datetime.strptime(known_args.end_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)

    p = beam.Pipeline(options=options)

    config = cmn.load_config(known_args.config)

    queries = create_queries(known_args)

    sources = [(p | "Read_{}".format(i) >> 
                        beam.io.Read(beam.io.gcp.bigquery.BigQuerySource(query=x, use_standard_sql=True)))
                        for (i, x) in enumerate(queries)]

    tagged_records = (sources
        | beam.Flatten()
        | cmn.CreateVesselRecords([], destination=None)
        | cmn.CreateTaggedRecords(config['min_port_events_positions'])
        )

    anchorages = (p
        | 'ReadAnchorages' >> QuerySource(anchorage_query.format(known_args.anchorage_table))
        | CreateTaggedAnchorages()
        )

    (tagged_records
        | CreateInOutEvents(anchorages=anchorages,
                            anchorage_entry_dist=config['anchorage_entry_distance_km'], 
                            anchorage_exit_dist=config['anchorage_exit_distance_km'], 
                            stopped_begin_speed=config['stopped_begin_speed_knots'],
                            stopped_end_speed=config['stopped_end_speed_knots'],
                            min_gap_minutes=config['minimum_port_gap_duration_minutes'])
        | "FilterEvents" >> Filter(lambda x: start_date.date() <= x.timestamp.date() <= end_date.date())
        | "writeInOutEvents" >> EventSink(table=known_args.output_table, 
                                          temp_location=cloud_options.temp_location,
                                          project=cloud_options.project)
        )


    result = p.run()

    success_states = set([PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN, PipelineState.PENDING])

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1


