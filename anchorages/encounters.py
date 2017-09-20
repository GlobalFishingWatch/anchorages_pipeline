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


from .common import VISITS_S2_SCALE, VISIT_SAFETY_FACTOR, approx_visit_cell_size, ANCHORAGES_S2_SCALE
from .common import filter_and_process_vessel_records, filter_duplicate_timestamps, VesselLocationRecord
from .common import find_visits, is_location_message, preset_runs, tag_with_destination_and_id, VesselMetadata
from .common import tagged_anchorage_visits_to_json, thin_points, LatLon, VesselInfoRecord, StationaryPeriod
from .common import check_that_pipeline_args_consumed, is_not_bad_value, Records_from_msg, tag_apts_with_nbr_s2ids
from .common import VesselMetadata_from_msg, LatLon_from_LatLng, S2_cell_ID, mean, LatLon_mean, datetime_to_text
from .common import single_anchorage_visit_to_json, add_pipeline_defaults, add_pipeline_defaults


import .common as cm


PseudoAnchorage = namedtuple("PseudoAnchorage", ['mean_location', "s2id", "port_name"])

def PseudoAnchorage_from_json(obj):
    return PseudoAnchorage(LatLon(obj['lat'], obj['lon']), obj['s2id'], Port._make(obj['port_name']))





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

    parser.add_argument('--start-date', required=True, help="First date to look for encounters starting.")

    parser.add_argument('--end-date', required=True, help="Last date (inclusive) to look for encounters starting.")

    parser.add_argument('--end-window', help="last date (inclusive) to look for encounters ending")



    parser.add_argument('--fishing-mmsi-list',
                         dest='fishing_mmsi_list',
                         default='../treniformis/treniformis/_assets/GFW/FISHING_MMSI/KNOWN_LIKELY_AND_SUSPECTED/ANY_YEAR.txt',
                         help='location of list of newline separated fishing mmsi')



    known_args, pipeline_args = parser.parse_known_args(argv)

    if known_args.output is None:
        known_args.output = 'gs://machine-learning-dev-ttl-30d/anchorages/{}/output/encounters'.format(known_args.name)

    add_pipeline_defaults(pipeline_args, known_args.name)

    with open(known_args.fishing_mmsi_list) as f:
        fishing_vessels = set([int(x.strip()) for x in f.readlines() if x.strip()])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    check_that_pipeline_args_consumed(pipeline_options)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_options)


    # TODO: Should go in config file or arguments >>>
    min_required_positions = 200 
    stationary_period_min_duration = datetime.timedelta(hours=12)
    stationary_period_max_distance = 0.5 # km
    min_unique_vessels_for_anchorage = 20
    blacklisted_mmsis = [0, 12345]
    anchorage_visit_max_distance = 3.0 # km

    assert anchorage_visit_max_distance + approx_visit_cell_size * VISIT_SAFETY_FACTOR < VISIT_BUFFER_KM

    anchorage_visit_min_duration = datetime.timedelta(minutes=180)
    # ^^^

    if known_args.input_patterns in preset_runs:
        raw_input_patterns = preset_runs[known_args.input_patterns]
    elif known_args.input_patterns is not None:
        raw_input_patterns = [x.strip() for x in known_args.input_patterns.split(',')]


    start_window = start_date = datetime.datetime.strptime(known_args.start_date, '%Y-%m-%d') 
    end_date = datetime.datetime.strptime(known_args.end_date, '%Y-%m-%d') 

    if known_args.end_window:
        end_window = datetime.datetime.strptime(known_args.end_window, '%Y-%m-%d')
    else:
        end_window = end_date + datetime.timedelta(days=1)

    input_patterns = []
    day = start_window
    while day <= end_window:
        for x in raw_input_patterns:
            input_patterns.append(x.format(date=day))
        day += datetime.timedelta(days=1)


    start_window = start_date = datetime.datetime.strptime(known_args.start_date, '%Y-%m-%d') 
    end_date = datetime.datetime.strptime(known_args.end_date, '%Y-%m-%d') 

    if known_args.end_window:
        end_window = datetime.datetime.strptime(known_args.end_window, '%Y-%m-%d')
    else:
        end_window = end_date + datetime.timedelta(days=1)


    ais_input_data_streams = [(p | 'ReadAis_{}'.format(i) >> ReadFromText(x)) for (i, x) in  enumerate(input_patterns)]

    ais_input_data = ais_input_data_streams | beam.Flatten() 

    location_records = (ais_input_data 
        | "ParseAis" >> beam.Map(json.loads)
        | "CreateLocationRecords" >> beam.FlatMap(Records_from_msg, blacklisted_mmsis)
        | "FilterByDateWindow" >> beam.Filter(lambda (md, lr): start_window <= lr.timestamp <= end_window)
        | "FilterOutBadValues" >> beam.Filter(lambda (md, lr): is_not_bad_value(lr))
        )


    grouped_records = (location_records 
        | "GroupByMmsi" >> beam.GroupByKey()
        | "OrderByTimestamp" >> beam.Map(lambda (md, records): (md, sorted(records, key=lambda x: x.timestamp)))
        )

    deduped_records = filter_duplicate_timestamps(grouped_records, min_required_positions)

    thinned_records =   ( deduped_records 
                        | "ThinPoints" >> beam.Map(lambda (md, vlrs): (md, list(thin_points(vlrs)))))

    tagged_records = ( thinned_records 
                     | "TagWithDestinationAndId" >> beam.Map(lambda (md, records): (md, tag_with_destination_and_id(records))))



    port_patterns = [x.strip() for x in known_args.port_patterns.split(',')]

    print("YYY", len(port_patterns))
    print(port_patterns)


    port_input_data_streams = [(p | 'ReadPort_{}'.format(i) >> ReadFromText(x)) for (i, x) in  enumerate(port_patterns)]


    if len(port_patterns) > 1:
        port_data = port_input_data_streams | "FlattenPorts" >> beam.Flatten()
    else:
        port_data = port_input_data_streams[0]


    anchorage_points = (port_data
        | "ParseAnchorages" >> beam.Map(json.loads)
        | "CreateAnchoragePoints" >> beam.Map(PseudoAnchorage_from_json)
        )


  # File "//anaconda/envs/dataflow/lib/python2.7/site-packages/apache_beam/transforms/core.py", line 751, in <lambda>
  # File "anchorages/encounters.py", line 391, in <lambda>
  #   | "TagWithDestinationAndId" >> beam.Map(lambda (md, records): (md, tag_with_destination_and_id(records))))
  # File "/usr/local/lib/python2.7/dist-packages/anchorages/common.py", line 165, in tag_with_destination_and_id
  #   s2id = S2_cell_ID(rcd.location).to_token()


    tagged_anchorage_points = ( anchorage_points 
                              | "tagAnchoragePointsWithNbrS2ids" >> beam.FlatMap(tag_apts_with_nbr_s2ids)
                              )

    # TODO: this is a very broad stationary distance.... is that what we want. Might be, but think about it.
    visit_records = filter_and_process_vessel_records(tagged_records, anchorage_visit_min_duration, anchorage_visit_max_distance,
                    prefix="anchorages")

    tagged_for_visits_records = ( visit_records  
                       | "TagSPWithS2id" >> beam.FlatMap(lambda (md, processed_locations): 
                                                [(s2sphere.CellId.from_token(sp.s2id).parent(VISITS_S2_SCALE).to_token(), 
                                                    (md, sp)) for sp in processed_locations.stationary_periods])
                       )


    anchorage_visits = ( (tagged_for_visits_records, tagged_anchorage_points)
                       | "CoGroupByS2id" >> beam.CoGroupByKey()
                       | "FindVisits"  >> beam.FlatMap(lambda (s2id, (md_sp_tuples, apts)): 
                                        (find_visits(s2id, md_sp_tuples, apts, anchorage_visit_max_distance)))  
                       | "FilterByDate" >> beam.Filter(lambda (md, (anch, sp)): start_date <= sp.start_time <= end_date)
                       | "GroupVisitsByMd" >> beam.GroupByKey()
                       )

    (anchorage_visits 
        | "convertAVToJson" >> beam.Map(tagged_anchorage_visits_to_json)
        | "writeAnchoragesVisits" >> WriteToText(known_args.output, file_name_suffix='.json')
    )


    result = p.run()
    result.wait_until_finish()

