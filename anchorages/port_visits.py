from __future__ import absolute_import, print_function, division

import argparse
import ujson as json
import json as classic_json
import datetime
from collections import namedtuple
import itertools as it
import s2sphere
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from . import common as cmn
from .nearest_port import AnchorageFinder
from .distance import distance, inf
from .transforms.source import Source
from .transforms.sink import EventSink


PseudoAnchorage = namedtuple("PseudoAnchorage", 
    ['mean_location', "s2id", "port_name"])

VisitEvent = namedtuple("VisitEvent", 
    ['anchorage_id', 'lat', 'lon', 'vessel_lat', 'vessel_lon', 'mmsi', 'timestamp', 'port_label', 'event_type'])

IN_PORT = "IN_PORT"
AT_SEA  = "AT_SEA"
STOPPED = "STOPPED"




class CreateTaggedPaths(cmn.CreateTaggedRecords):

    def tag_path(self, item):
        mmsi, records = item
        s2ids = set()
        for rcd in records:
            s2_cell_id = cmn.S2_cell_ID(rcd.location, cmn.VISITS_S2_SCALE)
            s2ids.add(s2_cell_id.to_token())
            for cell_id in s2_cell_id.get_all_neighbors(cmn.VISITS_S2_SCALE):
                s2ids.add(cell_id.to_token())
        return (mmsi, (s2ids, records))

    def expand(self, vessel_records):
        tagged_records = cmn.CreateTaggedRecords.expand(self, vessel_records)
        return (tagged_records
            | beam.Map(self.tag_path)
            )


class CreateTaggedAnchorages(beam.PTransform):

    def json_to_dict(self, text):
        # TODO: ujson was breaking here earlier. When things are stable, revisit 
        # and see if it's still a problem
        return classic_json.loads(text)

    def dict_to_psuedo_anchorage(self, obj):
        return PseudoAnchorage(cmn.LatLon(obj['anchor_lat'], obj['anchor_lon']), 
            obj['anchor_id'], (obj['FINAL_NAME'], obj['iso3']))

    def tag_anchorage_with_s2id(self, anchorage):
        s2id = s2sphere.CellId.from_token(anchorage.s2id).parent(cmn.VISITS_S2_SCALE).to_token()
        return (s2id, anchorage)

    def expand(self, anchorages_text):
        return (anchorages_text
            | beam.Map(self.json_to_dict)
            | beam.Map(self.dict_to_psuedo_anchorage)
            | beam.Map(self.tag_anchorage_with_s2id)
            | beam.CombineGlobally(CombineAnchoragesIntoMap())
            )


# Future state transitions
# IN_PORT
#    (dist >= exit_dist) -> AT_SEA
#    (dist < exit_dist & speed <= stopped_begin_speed) -> STOPPED
#    othewise unchanged
# AT_SEA
#    (dist < entry_dist & speed <= stopped_begin_speed) -> STOPPED
#    (dist < entry_dist & speed > stopped_begin_speed) -> IN_PORT
#    othewise unchanged
# STOPPED
#    (dist >= exit_dist) -> AT_SEA
#    (dist < exit_dist & speed > stopped_end_speed) -> IN_PORT
#    othewise unchanged


class CreateInOutEvents(beam.PTransform):

    transition_map = {
        (AT_SEA, AT_SEA)   : [],
        (AT_SEA, IN_PORT)  : ['PORT_ENTRY'],
        (AT_SEA, STOPPED)  : ['PORT_ENTRY', 'PORT_STOP'],
        (IN_PORT, AT_SEA)  : ['PORT_EXIT'],
        (IN_PORT, IN_PORT) : [],
        (IN_PORT, STOPPED) : ['PORT_STOP'],
        (STOPPED, AT_SEA)  : ['PORT_EXIT'],
        (STOPPED, IN_PORT) : [],
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
            return (state in (IN_PORT, STOPPED))

    def _is_stopped(self, state, speed):
        if speed <= self.stopped_begin_speed:
            return True
        elif speed >= self.stopped_end_speed:
            return False
        else:
            return (state == STOPPED) 

    def _anchorage_distance(self, loc, anchorages):
        closest = None
        min_dist = inf
        for anch in anchorages:
            dist = dist(loc, port.mean_location)
            if dist < min_dist:
                min_dist = dist
                closest = anch
        return closest, min_dist

    def create_in_out_events(self, tagged_path, anchorage_map):
        mmsi, (s2ids, records) = tagged_path
        state = None
        active_port = None
        anchorages = set()
        for x in s2ids:
            anchorages |= anchorage_map.get(x, set())
        finder = AnchorageFinder(list(anchorages))
        events = []

        buffer_dist = max(self.anchorage_entry_dist, self.anchorage_exit_dist)
        for rcd in records:
            # TODO: Tagg anchorages with neighbors instead of paths, then lookup
            # anchorages by s2id!
            port, dist = finder.find_nearest_port_and_distance(rcd.location)
            
            last_state = state
            is_in_port = self._is_in_port(state, dist)
            is_stopped = self._is_stopped(state, rcd.speed)

            if is_in_port:
                active_port = port
                state = STOPPED if is_stopped else IN_PORT
            else:
                state = AT_SEA

            if (last_state is None) or (active_port is None):
                # Not enough information yet.
                continue 

            for event_type in self.transition_map[(last_state, state)]:
                events.append(VisitEvent(anchorage_id=active_port.anchorage_point.s2id, 
                                         lat=active_port.lat, 
                                         lon=active_port.lon, 
                                         vessel_lat=rcd.location.lat,
                                         vessel_lon=rcd.location.lon,
                                         mmsi=mmsi, 
                                         timestamp=rcd.timestamp, 
                                         port_label=active_port.name, 
                                         event_type=event_type)) 
        return events

    def event_to_dict(self, event):
        return event._asdict()

    def expand(self, tagged_paths):
        anchorage_map = beam.pvalue.AsSingleton(self.anchorages)
        return (tagged_paths
            | beam.FlatMap(self.create_in_out_events, anchorage_map=anchorage_map)
            | beam.Map(self.event_to_dict)
            )



def parse_command_line_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--name', required=True, 
                        help='Name to prefix output and job name if not otherwise specified')
    parser.add_argument('--anchorage-path', 
                        help='Anchorage file pattern (glob)')
    parser.add_argument('--output', dest='output',
                        help='Output table to write results to.')
    parser.add_argument('--input-table', default='pipeline_classify_p_p429_resampling_2',
                        help='Input table to pull data from')
    parser.add_argument('--start-date', required=True, 
                        help="First date to look for entry/exit events.")
    parser.add_argument('--end-date', required=True, 
                        help="Last date (inclusive) to look for entry/exit events.")
    parser.add_argument('--start-window', 
                        help="date to start tracking events to warm up vessel state")
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


def create_query(args):
    template = """
    SELECT mmsi, lat, lon, timestamp, destination, speed FROM   
      TABLE_DATE_RANGE([world-fishing-827:{table}.], 
                        TIMESTAMP('{start:%Y-%m-%d}'), TIMESTAMP('{end:%Y-%m-%d}')) 
    """
    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d') 
    end_window = end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d') 
    if args.start_window:
        start_window = datetime.datetime.strptime(args.start_window, '%Y-%m-%d')
    else:
        start_window = start_date - datetime.timedelta(days=1)

    query = template.format(table=args.input_table, start=start_window, end=end_window)
    if args.fast_test:
        query += 'LIMIT 100000'

    return query


class CombineAnchoragesIntoMap(beam.CombineFn):

    def create_accumulator(self):
        return {}

    def add_input(self, accumulator, item):
        key, value = item
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


def run():
    known_args, pipeline_options = parse_command_line_args()

    p = beam.Pipeline(options=pipeline_options)

    config = cmn.load_config(known_args.config)

    query = create_query(known_args)

    tagged_paths = (p 
        | "ReadAis" >> Source(query)
        | cmn.CreateVesselRecords(config['blacklisted_mmsis'])
        | CreateTaggedPaths(config['min_required_positions'])
        )

    anchorages = (p
        | 'ReadAnchorages' >> ReadFromText(known_args.anchorage_path)
        | CreateTaggedAnchorages()
        )

    (tagged_paths
        | CreateInOutEvents(anchorages=anchorages,
                            anchorage_entry_dist=config['anchorage_entry_distance_km'], 
                            anchorage_exit_dist=config['anchorage_exit_distance_km'], 
                            stopped_begin_speed=config['stopped_begin_speed_knots'],
                            stopped_end_speed=config['stopped_end_speed_knots'])
        | "writeInOutEvents" >> EventSink(table=known_args.output, write_disposition="WRITE_APPEND")
        )

    result = p.run()
    result.wait_until_finish()

