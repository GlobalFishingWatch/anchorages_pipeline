from __future__ import absolute_import, print_function, division

import datetime
import logging

from . import common as cmn
from .records import VesselLocationRecord
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
    WITH 

    destinations AS (
      SELECT seg_id, _TABLE_SUFFIX AS table_suffix,
          CASE 
            WHEN ARRAY_LENGTH(destinations) = 0 THEN NULL
            ELSE (SELECT MAX(value)
                  OVER (ORDER BY count DESC)
                  FROM UNNEST(destinations)
                  LIMIT 1)
            END AS destination
      FROM `{segment_table}*`
      WHERE _TABLE_SUFFIX BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}'
    ),

    positions AS (
      SELECT ssvid, seg_id, lat, lon, timestamp, speed, 
             _TABLE_SUFFIX as table_suffix
        FROM `{position_table}*` 
       WHERE _TABLE_SUFFIX BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}'
         AND seg_id IS NOT NULL
         AND lat IS NOT NULL
         AND lon IS NOT NULL
         AND speed IS NOT NULL
    )

    SELECT ssvid as ident,
           lat,
           lon,
           timestamp,
           destination,
           speed
    FROM positions
    JOIN destinations
    USING (seg_id, table_suffix)
    """
    start_window = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_window = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')

    queries = []
    start = start_window
    while start < end_window:
        # Add 999 days so that we get 1000 total days
        end = min(start + datetime.timedelta(days=999), end_window)
        queries.append(template.format(position_table=args.messages_table,
                                       segment_table=args.segments_table,
                                       start=start, end=end))
        # Add 1 day to end, so that we don't overlap.
        start = end + datetime.timedelta(days=1)

    return queries


def has_location_record(item):
    _, rcd = item
    return isinstance(rcd, VesselLocationRecord)


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
        | cmn.CreateVesselRecords()
        | "FilterOutInfo" >> beam.Filter(has_location_record)
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

    success_states = set([PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN, PipelineState.PENDING])

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1

