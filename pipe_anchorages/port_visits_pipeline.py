from __future__ import absolute_import, print_function, division

import datetime
import logging
import pytz

import apache_beam as beam
from apache_beam import Map
from apache_beam import Filter
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import PipelineState
from apache_beam.transforms.window import TimestampedValue

from pipe_tools.io import WriteToBigQueryDatePartitioned

from . import common as cmn
from .transforms.source import QuerySource
from .objects.visit_event import VisitEvent
from .objects.namedtuples import _datetime_to_s
from .options.port_visits_options import PortVisitsOptions
from .schema.port_visit import build as build_visit_schema
from .transforms.create_port_visits import CreatePortVisits




def create_queries(args, start_date, end_date):
    template = """
    SELECT events.* except(timestamp, last_timestamp),
            CAST(UNIX_MICROS(events.timestamp) AS FLOAT64) / 1000000 AS timestamp,
            CAST(UNIX_MICROS(events.last_timestamp) AS FLOAT64) / 1000000 AS last_timestamp,
            ssvid, vessel_id
    FROM `{evt_table}*` events
    JOIN `{vid_table}` vids
    USING (seg_id)
    WHERE events._table_suffix BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}' 
    {condition}
    """
    if args.bad_segs_table is None:
        condition = ''
    else:
        condition = f'  AND seg_id NOT IN (SELECT seg_id FROM {args.bad_segs_table})'

    start_window = start_date
    shift = 1000
    while start_window <= end_date:
        end_window = min(start_window + datetime.timedelta(days=shift), end_date)
        query = template.format(evt_table=args.events_table, 
                                vid_table=args.vessel_id_table,
                                condition=condition,
                                start=start_window, end=end_window)
        yield query
        start_window = end_window + datetime.timedelta(days=1)

def from_msg(x):
    x['timestamp'] =  datetime.datetime.utcfromtimestamp(
            x['timestamp']).replace(tzinfo=pytz.utc)
    ssvid = x.pop('ssvid')
    vessel_id = x.pop('vessel_id')
    return (ssvid, vessel_id), VisitEvent(**x)

def event_to_msg(x):
    x = x._asdict()
    x['timestamp'] = _datetime_to_s(x['timestamp'])
    return x

def visit_to_msg(x):
    x = x._asdict()
    x['events'] = [event_to_msg(y) for y in x['events']]
    x['start_timestamp'] = _datetime_to_s(x['start_timestamp'])
    x['end_timestamp'] = _datetime_to_s(x['end_timestamp'])
    return x


def run(options):

    visit_args = options.view_as(PortVisitsOptions)
    cloud_args = options.view_as(GoogleCloudOptions)

    p = beam.Pipeline(options=options)

    start_date = datetime.datetime.strptime(visit_args.start_date, '%Y-%m-%d').replace(tzinfo=pytz.utc) 
    end_date = datetime.datetime.strptime(visit_args.end_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)

    dataset, table = visit_args.output_table.split('.') 

    sink = WriteToBigQueryDatePartitioned(
        temp_gcs_location=cloud_args.temp_location,
        dataset=dataset,
        table=table,
        project=cloud_args.project,
        write_disposition="WRITE_TRUNCATE",
        schema=build_visit_schema(),
        temp_shards_per_day=10
        )

    # TODO: add optional bad_segs input table (X)
    # TODO: read by all events per vessel_id by seg_id (x)
    # TODO: add duration to port visits

    queries = create_queries(visit_args, start_date, end_date)

    sources = [(p | "Read_{}".format(i) >> beam.io.Read(
                        beam.io.gcp.bigquery.BigQuerySource(query=x, use_standard_sql=True)))
                            for (i, x) in enumerate(queries)]

    tagged_records = (sources
        | beam.Flatten()
        | beam.Map(from_msg)
        | CreatePortVisits(visit_args.max_inter_seg_dist_nm)
        | Map(lambda x: TimestampedValue(visit_to_msg(x), _datetime_to_s(x.end_timestamp)))
        | sink
        )


    result = p.run()

    success_states = set([PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN, PipelineState.PENDING])

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1


