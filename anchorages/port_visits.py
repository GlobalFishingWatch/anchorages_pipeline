from __future__ import absolute_import, print_function, division

import argparse
import logging
import re
import ujson as json
import datetime
from collections import namedtuple, Counter, defaultdict
import itertools as it
import os
import math
import s2sphere
from .port_name_filter import normalized_valid_names
from .union_find import UnionFind
from .sparse_inland_mask import SparseInlandMask
from .distance import distance
from .nearest_port import port_finder, AnchorageFinder, BUFFER_KM as VISIT_BUFFER_KM, Port

# TODO put unit reg in package if we refactor
# import pint
# unit = pint.UnitRegistry()

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


from  . import common as cmn


PseudoAnchorage = namedtuple("PseudoAnchorage", ['mean_location', "s2id", "port_name"])

def PseudoAnchorage_from_json(obj):
    return PseudoAnchorage(cmn.LatLon(obj['lat'], obj['lon']), obj['s2id'], Port._make(obj['port_name']))


IN_PORT = "IN_PORT"
AT_SEA = "AT_SEA"



            # Anchorage point (s2 id, lat, lon)
            # Port Label
            # Timestamp
            # MMSI
            # Event Type (in or out)

VisitEvent = namedtuple("VisitEvent", 
    ['anchorage_point', 'mmsi', 'event_type'])


def find_in_out_events((md, records), port_entry_dist, port_exit_dist, anchorages):
    state = None
    current_port = None
    current_distance = None
    finder = AnchorageFinder(anchorages)
    events = []
    buffer_dist = max(port_entry_dist, port_exit_dist)
    for rcd in records:
        port, dist = finder.is_within_dist(buffer_dist, rcd)
        if port is not None:
            if dist < port_entry_dist:
                # We are in a port
                if state == AT_SEA:
                    # We were outside of a port; so entered a port
                    events.append((port, md.mmsi, "PORT_ENTRY")) # TODO: put real event
                state = IN_PORT
                if state != IN_PORT or dist < current_distance:
                    current_port = port
                    current_distance = dist
            elif dist > port_exit_dist:
                # We are outside a port
                if state == IN_PORT:
                    # We were in a port, so exited a port
                    events.append((port, md.mmsi, "PORT_EXIT")) 
                state = AT_SEA
                current_port = current_distance = None



def event_to_json(visit):
    return json.dumps(visit._asdict())


def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('--name', required=True, help='Name to prefix output and job name if not otherwise specified')

    parser.add_argument('--port-patterns', help='Input file patterns (comma separated) for anchorages output to process (glob)')
    parser.add_argument('--output',
                                            dest='output',
                                            help='Output file to write results to.')

    parser.add_argument('--input-patterns', default='custom',
                                            help='Input file to patterns (comma separated) to process (glob)')

    parser.add_argument('--start-date', required=True, help="First date to look for entry/exit events.")

    parser.add_argument('--end-date', required=True, help="Last date (inclusive) to look for entry/exit events.")

    parser.add_argument('--start-window', help="date to start tracking events to warm up vessel state")



    parser.add_argument('--fishing-mmsi-list',
                         dest='fishing_mmsi_list',
                         default='../treniformis/treniformis/_assets/GFW/FISHING_MMSI/KNOWN_LIKELY_AND_SUSPECTED/ANY_YEAR.txt',
                         help='location of list of newline separated fishing mmsi')



    known_args, pipeline_args = parser.parse_known_args(argv)

    if known_args.output is None:
        known_args.output = 'gs://machine-learning-dev-ttl-30d/anchorages/{}/output/encounters'.format(known_args.name)

    cmn.add_pipeline_defaults(pipeline_args, known_args.name)

    with open(known_args.fishing_mmsi_list) as f:
        fishing_vessels = set([int(x.strip()) for x in f.readlines() if x.strip()])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    cmn.check_that_pipeline_args_consumed(pipeline_options)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_options)


    # TODO: Should go in config file or arguments >>>
    min_required_positions = 200 
    stationary_period_min_duration = datetime.timedelta(hours=12)
    stationary_period_max_distance = 0.5 # km
    min_unique_vessels_for_anchorage = 20
    blacklisted_mmsis = [0, 12345]
    anchorage_visit_max_distance = 3.0 # km

    assert anchorage_visit_max_distance + cmn.approx_visit_cell_size * cmn.VISIT_SAFETY_FACTOR < VISIT_BUFFER_KM

    anchorage_visit_min_duration = datetime.timedelta(minutes=180)
    # ^^^

    #     # XXX TODO: Put back...
    # if known_args.input_patterns in cmn.preset_runs:
    #     raw_input_patterns = cmn.preset_runs[known_args.input_patterns]
    # elif known_args.input_patterns is not None:
    #     raw_input_patterns = known_args.input_patterns.split(',')]
    raw_input_patterns = [x.strip() for x in ['gs://p_p429_resampling_3/data-production/classify-pipeline/classify/{date:%Y-%m-%d}/*0-of-*']]


    start_date = datetime.datetime.strptime(known_args.start_date, '%Y-%m-%d') 
    end_window = end_date = datetime.datetime.strptime(known_args.end_date, '%Y-%m-%d') 

    if known_args.start_window:
        start_window = datetime.datetime.strptime(known_args.start_window, '%Y-%m-%d')
    else:
        start_window = start_date - datetime.timedelta(days=1)

    input_patterns = []
    day = start_window
    while day <= end_window:
        for x in raw_input_patterns:
            input_patterns.append(x.format(date=day))
        day += datetime.timedelta(days=1)


    start_date = datetime.datetime.strptime(known_args.start_date, '%Y-%m-%d') 
    end_window = end_date = datetime.datetime.strptime(known_args.end_date, '%Y-%m-%d') 


    ais_input_data_streams = [(p | 'ReadAis_{}'.format(i) >> ReadFromText(x)) for (i, x) in  enumerate(input_patterns)]

    ais_input_data = ais_input_data_streams | beam.Flatten() 

    location_records = (ais_input_data 
        | "ParseAis" >> beam.Map(json.loads)
        | "CreateLocationRecords" >> beam.FlatMap(cmn.Records_from_msg, blacklisted_mmsis)
        | "FilterByDateWindow" >> beam.Filter(lambda (md, lr): start_window <= lr.timestamp <= end_window)
        | "FilterOutBadValues" >> beam.Filter(lambda (md, lr): cmn.is_not_bad_value(lr))
        )

    # mmsi -> s2id list; 
    # s2idlist -> anchorages per mmsi
    # do cogroup on mmsi


    grouped_records = (location_records 
        | "GroupByMmsi" >> beam.GroupByKey()
        | "OrderByTimestamp" >> beam.Map(lambda (md, records): (md, sorted(records, key=lambda x: x.timestamp)))
        )

    deduped_records = cmn.filter_duplicate_timestamps(grouped_records, min_required_positions)

    thinned_records =   ( deduped_records 
                        | "ThinPoints" >> beam.Map(lambda (md, vlrs): (md, list(cmn.thin_points(vlrs)))))

    tagged_records = ( thinned_records 
                     | "TagWithDestinationAndId" >> beam.Map(lambda (md, records): (md, cmn.tag_with_destination_and_id(records))))

    port_patterns = [x.strip() for x in known_args.port_patterns.split(',')]


    port_input_data_streams = [(p | 'ReadPort_{}'.format(i) >> ReadFromText(x)) for (i, x) in  enumerate(port_patterns)]


    port_data = beam.pvalue.AsIter(port_input_data_streams 
        | "FlattenPorts" >> beam.Flatten() 
        | "JsonToPortsDict" >> beam.Map(json.loads)
        | "CreatePorts" >> beam.Map(PseudoAnchorage_from_json))


    events = (tagged_records | beam.FlatMap(find_in_out_events, port_entry_dist=3.0, port_exit_dist=4.0, anchorages=port_data))

    (events
        | "convertAVToJson" >> beam.Map(event_to_json)
        | "writeAnchoragesVisits" >> WriteToText(known_args.output, file_name_suffix='.json'))


    result = p.run()
    result.wait_until_finish()

