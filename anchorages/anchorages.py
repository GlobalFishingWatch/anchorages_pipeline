from __future__ import absolute_import, print_function, division

import argparse
import ujson as json
import datetime
import s2sphere
from .nearest_port import BUFFER_KM as VISIT_BUFFER_KM


import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from .common import VISITS_S2_SCALE, VISIT_SAFETY_FACTOR, anchorage_point_to_json, approx_visit_cell_size
from .common import filter_and_process_vessel_records, filter_duplicate_timestamps, find_anchorage_points
from .common import find_visits, is_location_message, preset_runs, read_json_records, tag_with_destination_and_id
from .common import tagged_anchorage_visits_to_json, thin_points, filter_by_latlon
from .common import check_that_pipeline_args_consumed, tag_apts_with_nbr_s2ids



def add_pipeline_defaults(pipeline_args, name):

    defaults = {
        '--project' : 'world-fishing-827',
        '--staging_location' : 'gs://machine-learning-dev-ttl-30d/anchorages/{}/output/staging'.format(name),
        '--temp_location' : 'gs://machine-learning-dev-ttl-30d/anchorages/temp',
        '--setup_file' : './setup.py',
        '--runner': 'DataflowRunner',
        '--max_num_workers' : '200',
        '--job_name': name,
    }

    for name, value in defaults.items():
        if name not in pipeline_args:
            pipeline_args.extend((name, value))



def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('--name', required=True, help='Name to prefix output and job name if not otherwise specified')

    parser.add_argument('--input-patterns', default='all_years',
                                            help='Input file to patterns (comma separated) to process (glob)')
    parser.add_argument('--output',
                                            dest='output',
                                            help='Output file to write results to.')

    parser.add_argument('--fishing-mmsi-list',
                         dest='fishing_mmsi_list',
                         default='../treniformis/treniformis/_assets/GFW/FISHING_MMSI/KNOWN_LIKELY_AND_SUSPECTED/ANY_YEAR.txt',
                         help='location of list of newline separated fishing mmsi')

    parser.add_argument('--latlon-filters',
                                            dest='latlon_filter',
                                            help='newline separated json file containing dicts of min_lat, max_lat, min_lon, max_lon, name')

    known_args, pipeline_args = parser.parse_known_args(argv)

    if known_args.output is None:
        known_args.output = 'gs://machine-learning-dev-ttl-30d/anchorages/{}/output'.format(known_args.name)

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
        input_patterns = preset_runs[known_args.input_patterns]
    else:
        input_patterns = [x.strip() for x in known_args.input_patterns.split(',')]

    ais_input_data_streams = [(p | 'read_{}'.format(i) >> ReadFromText(x)) for (i, x) in  enumerate(input_patterns)]

    ais_input_data = ais_input_data_streams | beam.Flatten()

    if known_args.latlon_filter:
        with open(known_args.latlon_filter) as f:
            latlon_filters = [json.loads(x) for x in f.readlines()]
    else:
        latlon_filters = None


    location_records = read_json_records(ais_input_data, blacklisted_mmsis, latlon_filters)


    grouped_records = (location_records 
        | "GroupByMmsi" >> beam.GroupByKey()
        | "OrderByTimestamp" >> beam.Map(lambda (md, records): (md, sorted(records, key=lambda x: x.timestamp)))
        )

    deduped_records = filter_duplicate_timestamps(grouped_records, min_required_positions)

    thinned_records =   ( deduped_records 
                        | "ThinPoints" >> beam.Map(lambda (md, vlrs): (md, list(thin_points(vlrs)))))

    tagged_records = ( thinned_records 
                     | "TagWithDestinationAndId" >> beam.Map(lambda (md, records): (md, tag_with_destination_and_id(records))))

    processed_records = filter_and_process_vessel_records(tagged_records, stationary_period_min_duration, stationary_period_max_distance) 

    anchorage_points = (find_anchorage_points(processed_records, min_unique_vessels_for_anchorage, fishing_vessels) 
        | 'filterOutInlandPorts' >> beam.Filter(lambda x: not inland_mask.query(x.mean_location)))


    (anchorage_points 
        | "convertAPToJson" >> beam.Map(anchorage_point_to_json)
        | "writeAnchoragesPoints" >> WriteToText(known_args.output + '/anchorages_points', file_name_suffix='.json')
    )

    # TODO: this is a very broad stationary distance.... is that what we want. Might be, but think about it.
    visit_records = filter_and_process_vessel_records(tagged_records, anchorage_visit_min_duration, anchorage_visit_max_distance,
                    prefix="anchorages")

    tagged_anchorage_points = ( anchorage_points 
                              | "tagAnchoragePointsWithNbrS2ids" >> beam.FlatMap(tag_apts_with_nbr_s2ids)
                              )

    tagged_for_visits_records = ( visit_records  
                       | "TagSPWithS2id" >> beam.FlatMap(lambda (md, processed_locations): 
                                                [(s2sphere.CellId.from_token(sp.s2id).parent(VISITS_S2_SCALE).to_token(), 
                                                    (md, sp)) for sp in processed_locations.stationary_periods])
                       )


    anchorage_visits = ( (tagged_for_visits_records, tagged_anchorage_points)
                       | "CoGroupByS2id" >> beam.CoGroupByKey()
                       | "FindVisits"  >> beam.FlatMap(lambda (s2id, (md_sp_tuples, apts)): 
                                        (find_visits(s2id, md_sp_tuples, apts, anchorage_visit_max_distance)))  
                       | "GroupVisitsByMd" >> beam.GroupByKey()
                       )

    (anchorage_visits 
        | "convertAVToJson" >> beam.Map(tagged_anchorage_visits_to_json)
        | "writeAnchoragesVisits" >> WriteToText(known_args.output + '/anchorages_visits', file_name_suffix='.json')
    )


    result = p.run()
    result.wait_until_finish()

