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


def from_msg(x):
    x['timestamp'] =  datetime.datetime.utcfromtimestamp(
            x['timestamp']).replace(tzinfo=pytz.utc)
    return VisitEvent(**x)


def event_to_msg(x):
    x = x._asdict()
    x['timestamp'] = _datetime_to_s(x['timestamp'])
    return x

def visit_to_msg(x):
    x = x._asdict()
    x['ssvid'] = x['track_id'].split('-')[0]
    x['events'] = [event_to_msg(y) for y in x['events']]
    x['start_timestamp'] = _datetime_to_s(x['start_timestamp'])
    x['end_timestamp'] = _datetime_to_s(x['end_timestamp'])
    return x


def run(options):

    visit_args = options.view_as(PortVisitsOptions)
    cloud_args = options.view_as(GoogleCloudOptions)

    p = beam.Pipeline(options=options)

    start_date = datetime.datetime.strptime(visit_args.initial_data_date, '%Y-%m-%d').replace(tzinfo=pytz.utc) 
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


    queries = VisitEvent.create_queries(visit_args.events_table, start_date, end_date)

    sources = [(p | "Read_{}".format(i) >> beam.io.Read(
                        beam.io.gcp.bigquery.BigQuerySource(query=x)))
                            for (i, x) in enumerate(queries)]

    tagged_records = (sources
        | beam.Flatten()
        | beam.Map(from_msg)
        | CreatePortVisits()
        | "FilterVisits" >> Filter(lambda x: start_date.date() <= x.end_timestamp.date() <= end_date.date())
        | Map(lambda x: TimestampedValue(visit_to_msg(x), 
                                        _datetime_to_s(x.end_timestamp)))
        | sink
        )


    result = p.run()

    success_states = set([PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN, PipelineState.PENDING])

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1


