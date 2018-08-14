from __future__ import absolute_import, print_function, division

import datetime
import logging

from . import common as cmn
from .transforms.source import QuerySource
from .transforms.sink import AnchorageSink
from .port_name_filter import normalized_valid_names
from .find_anchorage_points import FindAnchoragePoints
from .options.anchorage_options import AnchorageOptions

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import PipelineState




def create_queries(args):
    template = """
    SELECT a.ssvid as vessel_id, 
           lat, 
           lon, 
           a.timestamp as timestamp, 
           destination, 
           speed
    FROM
      (SELECT *, _TABLE_SUFFIX FROM `{position_table}*` 
        WHERE _TABLE_SUFFIX BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}') a
    INNER JOIN
      (SELECT *, _TABLE_SUFFIX FROM `{segment_table}*` 
        WHERE _TABLE_SUFFIX BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}' AND
        noise = FALSE) b
    USING(_TABLE_SUFFIX, seg_id)
    """
    start_window = datetime.datetime.strptime(args.start_date, '%Y-%m-%d') 
    end_window = datetime.datetime.strptime(args.end_date, '%Y-%m-%d') 
    table = args.input_dataset + '.messages_segmented_'
    segment_table = args.input_dataset + '.segments_'

    queries = []
    start = start_window
    while start < end_window:
        # Add 999 days so that we get 1000 total days
        end = min(start + datetime.timedelta(days=999), end_window)
        queries.append(template.format(position_table=table, 
                                       segment_table=segment_table,
                                       min_message_count=2,
                                       start=start, end=end))
        # Add 1 day to end, so that we don't overlap.
        start = end + datetime.timedelta(days=1)

    return queries



def run(options):
    known_args = options.view_as(AnchorageOptions)
    cloud_options = options.view_as(GoogleCloudOptions)

    config = cmn.load_config(known_args.config)

    queries = create_queries(known_args)


    p = beam.Pipeline(options=options)

    fishing_vessels = p | beam.io.ReadFromText(known_args.fishing_ssvid_list)
    fishing_vessel_list = beam.pvalue.AsList(fishing_vessels)

    source = [(p | "Source_{}".format(i) >> QuerySource(query, use_standard_sql=True)) 
                for (i, query) in enumerate(queries)] | beam.Flatten()

    tagged_records = (source
        | cmn.CreateVesselRecords(config['blacklisted_ssvids'])
        | cmn.CreateTaggedRecords(config['min_required_positions'])
        )

    anchorage_points = (tagged_records
        | FindAnchoragePoints(datetime.timedelta(minutes=config['stationary_period_min_duration_minutes']), 
                              config['stationary_period_max_distance_km'],
                              config['min_unique_vessels_for_anchorage'],
                              fishing_vessel_list)
        )

    (anchorage_points | AnchorageSink(table=known_args.output_table, 
                                      write_disposition="WRITE_TRUNCATE")
    )

    result = p.run()

    success_states = set([PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN])

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1

