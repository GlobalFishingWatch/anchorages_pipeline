from __future__ import absolute_import, print_function, division

import argparse
import datetime
from collections import namedtuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from . import common as cmn
from .distance import distance, inf
from .transforms.source import QuerySource
from .transforms.sink import EventSink


PseudoAnchorage = namedtuple("PseudoAnchorage", 
    ['mean_location', "s2id", "port_name"])

VisitEvent = namedtuple("VisitEvent", 
    ['anchorage_id', 'lat', 'lon', 'vessel_lat', 'vessel_lon', 'mmsi', 'timestamp', 'port_label', 'event_type'])


class CreateTaggedAnchorages(beam.PTransform):

    def dict_to_psuedo_anchorage(self, obj):
        return PseudoAnchorage(
                mean_location = cmn.LatLon(obj['anchor_lat'], obj['anchor_lon']), 
                s2id = obj['anchor_id'], 
                port_name = obj['FINAL_NAME'])

    def tag_anchorage_with_s2ids(self, anchorage):
        central_cell_id = anchorage.mean_location.S2CellId(cmn.VISITS_S2_SCALE)
        s2ids = {central_cell_id.to_token()}
        for cell_id in central_cell_id.get_all_neighbors(cmn.VISITS_S2_SCALE):
            s2ids.add(cell_id.to_token())
        return (s2ids, anchorage)

    def expand(self, anchorages_text):
        return (anchorages_text
            | beam.Map(self.dict_to_psuedo_anchorage)
            | beam.Map(self.tag_anchorage_with_s2ids)
            | beam.CombineGlobally(CombineAnchoragesIntoMap())
            )



class CreateInOutEvents(beam.PTransform):

    IN_PORT = "IN_PORT"
    AT_SEA  = "AT_SEA"
    STOPPED = "STOPPED"

    EVT_ENTER = 'PORT_ENTRY'
    EVT_EXIT  = 'PORT_EXIT'
    EVT_STOP  = 'PORT_STOP_BEGIN'
    EVT_START = 'PORT_STOP_END'

    transition_map = {
        (AT_SEA, AT_SEA)   : [],
        (AT_SEA, IN_PORT)  : [EVT_ENTER],
        (AT_SEA, STOPPED)  : [EVT_ENTER, EVT_STOP],
        (IN_PORT, AT_SEA)  : [EVT_EXIT],
        (IN_PORT, IN_PORT) : [],
        (IN_PORT, STOPPED) : [EVT_STOP],
        (STOPPED, AT_SEA)  : [EVT_START, EVT_EXIT],
        (STOPPED, IN_PORT) : [EVT_START],
        (STOPPED, STOPPED) : [],
    }

    def __init__(self, anchorages, 
                 anchorage_entry_dist, anchorage_exit_dist,
                 stopped_begin_speed, stopped_end_speed):
        self.anchorages = anchorages
        self.anchorage_entry_dist = anchorage_entry_dist
        self.anchorage_exit_dist = anchorage_exit_dist
        self.stopped_begin_speed = stopped_begin_speed
        self.stopped_end_speed = stopped_end_speed

    def _is_in_port(self, state, dist):
        if dist <= self.anchorage_entry_dist:
            return True
        elif dist >= self.anchorage_exit_dist:
            return False
        else:
            return (state in (self.IN_PORT, self.STOPPED))

    def _is_stopped(self, state, speed):
        if speed <= self.stopped_begin_speed:
            return True
        elif speed >= self.stopped_end_speed:
            return False
        else:
            return (state == self.STOPPED) 

    def _anchorage_distance(self, loc, anchorages):
        closest = None
        min_dist = inf
        for anch in anchorages:
            dist = distance(loc, anch.mean_location)
            if dist < min_dist:
                min_dist = dist
                closest = anch
        return closest, min_dist

    def create_in_out_events(self, tagged_records, anchorage_map):
        mmsi, records = tagged_records
        state = None
        active_port = None
        events = []
        for rcd in records:
            s2id = rcd.location.S2CellId(cmn.VISITS_S2_SCALE).to_token()
            port, dist = self._anchorage_distance(rcd.location, anchorage_map.get(s2id, []))

            last_state = state
            is_in_port = self._is_in_port(state, dist)
            is_stopped = self._is_stopped(state, rcd.speed)

            if is_in_port:
                active_port = port
                state = self.STOPPED if is_stopped else self.IN_PORT
            else:
                state = self.AT_SEA

            if (last_state is None) or (active_port is None):
                # Not enough information yet.
                continue 

            for event_type in self.transition_map[(last_state, state)]:
                events.append(VisitEvent(anchorage_id=active_port.s2id, 
                                         lat=active_port.mean_location.lat, 
                                         lon=active_port.mean_location.lon, 
                                         vessel_lat=rcd.location.lat,
                                         vessel_lon=rcd.location.lon,
                                         mmsi=mmsi, 
                                         timestamp=rcd.timestamp, 
                                         port_label=active_port.port_name, 
                                         event_type=event_type)) 
        return events

    def expand(self, tagged_records):
        anchorage_map = beam.pvalue.AsSingleton(self.anchorages)
        return (tagged_records
            | beam.FlatMap(self.create_in_out_events, anchorage_map=anchorage_map)
            )



def parse_command_line_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--name', required=True, 
                        help='Name to prefix output and job name if not otherwise specified')
    parser.add_argument('--anchorages', 
                        help='Name of of anchorages table (BQ)')
    parser.add_argument('--output', dest='output',
                        help='Output table (BQ) to write results to.')
    parser.add_argument('--input-table', default='pipeline_classify_p_p429_resampling_2',
                        help='Input table to pull data from')
    parser.add_argument('--start-date', required=True, 
                        help="First date to look for entry/exit events.")
    parser.add_argument('--end-date', required=True, 
                        help="Last date (inclusive) to look for entry/exit events.")
    parser.add_argument('--config', default='config.yaml',
                        help="path to configuration file")
    parser.add_argument('--fast-test', action='store_true', 
                        help='limit query size for testing')

    known_args, pipeline_args = parser.parse_known_args()

    if known_args.output is None:
        known_args.output = 'machine_learning_dev_ttl_30d.in_out_events_{}'.format(known_args.name)

    cmn.add_pipeline_defaults(pipeline_args, known_args.name)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    cmn.check_that_pipeline_args_consumed(pipeline_options)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    return known_args, pipeline_options


def create_queries(args):
    template = """
    SELECT mmsi, lat, lon, timestamp, destination, speed FROM   
      TABLE_DATE_RANGE([world-fishing-827:{table}.], 
                        TIMESTAMP('{start:%Y-%m-%d}'), TIMESTAMP('{end:%Y-%m-%d}')) 
    """
    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d') 
    end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d') 
    while start_date <= end_date:
        start_window = start_date - datetime.timedelta(days=1)
        end_of_year = datetime.datetime(year=start_date.year, month=12, day=31)
        end_window = min(end_date, end_of_year)
        query = template.format(table=args.input_table, start=start_window, end=end_window)
        if args.fast_test:
            query += 'LIMIT 100000'
        yield query
        start_date = datetime.datetime(year=start_date.year + 1, month=1, day=1)


class CombineAnchoragesIntoMap(beam.CombineFn):

    def create_accumulator(self):
        return {}

    def add_input(self, accumulator, item):
        keys, value = item
        for key in keys:
            if key not in accumulator:
                accumulator[key] = set()
            accumulator[key].add(value)
        return accumulator

    def merge_accumulators(self, accumulators):
        result = {}
        for accum in accumulators:
            for (key, val) in accum.iteritems():
                if key not in result:
                    result[key] = set()
                result[key] |= val
        return result

    def extract_output(self, accumulator):
        return accumulator


anchorage_query = 'SELECT lat anchor_lat, lon anchor_lon, anchor_id, FINAL_NAME FROM [{}]'


def run():
    known_args, pipeline_options = parse_command_line_args()

    p = beam.Pipeline(options=pipeline_options)

    config = cmn.load_config(known_args.config)

    queries = create_queries(known_args)

    sources = [(p | "Read_{}".format(i) >> beam.io.Read(beam.io.gcp.bigquery.BigQuerySource(query=x)))
                        for (i, x) in enumerate(queries)]

    tagged_records = (sources
        | beam.Flatten()
        | cmn.CreateVesselRecords(config['blacklisted_mmsis'])
        | cmn.CreateTaggedRecords(config['min_required_positions'])
        )

    anchorages = (p
        | 'ReadAnchorages' >> QuerySource(anchorage_query.format(known_args.anchorages))
        | CreateTaggedAnchorages()
        )

    (tagged_records
        | CreateInOutEvents(anchorages=anchorages,
                            anchorage_entry_dist=config['anchorage_entry_distance_km'], 
                            anchorage_exit_dist=config['anchorage_exit_distance_km'], 
                            stopped_begin_speed=config['stopped_begin_speed_knots'],
                            stopped_end_speed=config['stopped_end_speed_knots'])
        | "writeInOutEvents" >> EventSink(table=known_args.output, write_disposition="WRITE_APPEND")
        )

    result = p.run()
    result.wait_until_finish()

