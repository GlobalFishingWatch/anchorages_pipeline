from __future__ import absolute_import, print_function, division

import argparse
import yaml
import ujson as json
import json as classic_json
import datetime
from collections import namedtuple
import itertools as it
import s2sphere
from .nearest_port import AnchorageFinder, BUFFER_KM as VISIT_BUFFER_KM

# TODO put unit reg in package if we refactor
# import pint
# unit = pint.UnitRegistry()

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from  . import common as cmn
from .transforms.source import Source
from .transforms.sink import EventSink


PseudoAnchorage = namedtuple("PseudoAnchorage", 
    ['mean_location', "s2id", "port_name"])

# Note if we subclass, we should use __slots__ = ()
VisitEvent = namedtuple("VisitEvent", 
    ['anchorage_id', 'lat', 'lon', 'mmsi', 'timestamp', 'port_label', 'event_type'])

# TODO: add as attribute VisitEvent
IN_PORT = "IN_PORT"
AT_SEA = "AT_SEA"

class CreateVesselRecords(beam.PTransform):

    def __init__(self, blacklisted_mmsis):
        self.blacklisted_mmsis = blacklisted_mmsis

    def from_msg(self, msg):
        obj = cmn.VesselRecord.from_msg(msg)
        if (obj is None) or (obj[0] in self.blacklisted_mmsis):
            return []
        else:
            return [obj]

    def expand(self, ais_source):
        return (ais_source
            | beam.FlatMap(self.from_msg)
        )


class CreateTaggedPaths(beam.PTransform):

    def __init__(self, min_required_positions):
        self.min_required_positions = min_required_positions
        self.FIVE_MINUTES = datetime.timedelta(minutes=5)

    def order_by_timestamp(self, item):
        mmsi, records = item
        records = list(records)
        records.sort(key=lambda x: x.timestamp)
        return mmsi, records

    def dedup_by_timestamp(self, item):
        key, source = item
        seen = set()
        sink = []
        for x in source:
            if x.timestamp not in seen:
                sink.append(x)
                seen.add(x.timestamp)
        return (key, sink)

    def long_enough(self, item):
        mmsi, records = item
        return len(records) >= self.min_required_positions

    def thin_records(self, item):
        mmsi, records = item
        last_timestamp = datetime.datetime(datetime.MINYEAR, 1, 1)
        thinned = []
        for rcd in records:
            if (rcd.timestamp - last_timestamp) >= self.FIVE_MINUTES:
                last_timestamp = rcd.timestamp
                thinned.append(rcd)
        return mmsi, thinned

    def tag_records(self, item):
        mmsi, records = item
        # TODO: reimplement that here
        return (mmsi, cmn.tag_with_destination_and_id(records))

    def tag_path(self, item):
        mmsi, records = item
        s2ids = set()
        for rcd in records:
            s2_cell_id = s2sphere.CellId.from_token(rcd.s2id).parent(cmn.VISITS_S2_SCALE)
            s2ids.add(s2_cell_id.to_token())
            for cell_id in s2_cell_id.get_all_neighbors(cmn.VISITS_S2_SCALE):
                s2ids.add(cell_id.to_token())
        return (mmsi, (s2ids, records))

    def expand(self, vessel_records):
        return (vessel_records
            | beam.GroupByKey()
            | beam.Map(self.order_by_timestamp)
            | beam.Map(self.dedup_by_timestamp)
            | beam.Filter(self.long_enough)
            | beam.Map(self.thin_records)
            | beam.Map(self.tag_records)
            | beam.Map(self.tag_path)
            )


class CreateTaggedAnchorages(beam.PTransform):

    def json_to_dict(self, text):
        # TODO: ujson was breaking here earlier. When things are stable, revisit 
        # and see if it's still a problem
        return classic_json.loads(text)

    def dict_to_psuedo_anchorages(self, obj):
        return PseudoAnchorage(cmn.LatLon(obj['anchor_lat'], obj['anchor_lon']), obj['anchor_id'], (obj['FINAL_NAME'], obj['iso3']))

    def tag_psuedo_anchorages_with_s2id(self, anchorage):
        s2id = s2sphere.CellId.from_token(anchorage.s2id).parent(cmn.VISITS_S2_SCALE).to_token()
        return (s2id, anchorage)

    def expand(self, anchorages_text):
        return (anchorages_text
            | beam.Map(self.json_to_dict)
            | beam.Map(self.dict_to_psuedo_anchorages)
            | beam.Map(self.tag_psuedo_anchorages_with_s2id)
            )



class CreateInOutEvents(beam.PTransform):

    def __init__(self, anchorages, anchorage_entry_dist, anchorage_exit_dist):
        self.anchorages = anchorages
        self.anchorage_entry_dist = anchorage_entry_dist
        self.anchorage_exit_dist = anchorage_exit_dist

    def create_in_out_events(self, tagged_path, anchorage_map):
        mmsi, (s2ids, records) = tagged_path
        state = None
        current_port = None
        anchorages = [anchorage_map[x] for x in s2ids if x in anchorage_map]
        finder = AnchorageFinder(anchorages)
        events = []
        buffer_dist = max(self.anchorage_entry_dist, self.anchorage_exit_dist)
        for rcd in records:
            port, dist = finder.is_within_dist(buffer_dist, rcd)
            if port is not None and dist < self.anchorage_entry_dist:
                # We are in a port
                if state == AT_SEA:
                    # We were outside of a port; so entered a port
                    events.append(VisitEvent(anchorage_id=port.anchorage_point.s2id, 
                                             lat=port.lat, 
                                             lon=port.lon, 
                                             mmsi=mmsi, 
                                             timestamp=rcd.timestamp, 
                                             port_label=port.name, 
                                             event_type="PORT_ENTRY")) 
                current_port = port
                state = IN_PORT
            elif port is None or dist > self.anchorage_exit_dist:
                # We are outside a port
                if state == IN_PORT:
                    # We were in a port, so exited a port
                    events.append(VisitEvent(anchorage_id=current_port.anchorage_point.s2id, 
                                             lat=current_port.lat, 
                                             lon=current_port.lon, 
                                             mmsi=mmsi, 
                                             timestamp=rcd.timestamp, 
                                             port_label=current_port.name, 
                                             event_type="PORT_EXIT")) 
                state = AT_SEA
                current_port = None
        return events

    def event_to_json(self, event):
        event = event._replace(timestamp = cmn.datetime_to_text(event.timestamp))
        return json.dumps(event._asdict())

    def expand(self, tagged_paths):
        anchorage_map = beam.pvalue.AsDict(self.anchorages)
        return (tagged_paths
            | beam.FlatMap(self.create_in_out_events, anchorage_map=anchorage_map)
            # | beam.Map(self.event_to_json)
            | beam.Map(lambda x: x._asdict())
            )



def parse_command_line_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--name', required=True, 
                        help='Name to prefix output and job name if not otherwise specified')
    parser.add_argument('--anchorage-path', 
                        help='Anchorage file pattern (glob)')
    parser.add_argument('--output', dest='output',
                        help='Output file to write results to.')
    parser.add_argument('--input-table', default='pipeline_classify_p_p429_resampling_2',
                        help='Input table to pull data from')
    parser.add_argument('--start-date', required=True, 
                        help="First date to look for entry/exit events.")
    parser.add_argument('--end-date', required=True, 
                        help="Last date (inclusive) to look for entry/exit events.")
    parser.add_argument('--start-window', 
                        help="date to start tracking events to warm up vessel state")
    # parser.add_argument('--shard-output', type=bool, default=False, 
    #                     help="Whether to shard output or dump as single file")
    parser.add_argument('--config', default='config.yaml',
                        help="path to configuration file")
    parser.add_argument('--fishing-mmsi-list', dest='fishing_mmsi_list',
                         default='../treniformis/treniformis/_assets/GFW/FISHING_MMSI/KNOWN_LIKELY_AND_SUSPECTED/ANY_YEAR.txt',
                         help='location of list of newline separated fishing mmsi')
    parser.add_argument('--fast-test', action='store_true', 
                        help='limit query size for testing')

    known_args, pipeline_args = parser.parse_known_args()

    if known_args.output is None:
        known_args.output = 'scratch_tim.in_out_events_{}'.format(known_args.name)

    cmn.add_pipeline_defaults(pipeline_args, known_args.name)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    cmn.check_that_pipeline_args_consumed(pipeline_options)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    return known_args, pipeline_options


def load_config(path):
    with open(path) as f:
        config = yaml.load(f.read())

    anchorage_visit_max_distance = max(config['anchorage_entry_distance_km'],
                                       config['anchorage_exit_distance_km'])
    assert (anchorage_visit_max_distance + 
            cmn.approx_visit_cell_size * cmn.VISIT_SAFETY_FACTOR < VISIT_BUFFER_KM)

    return config


def create_query(args):
    template = """
    SELECT mmsi, lat, lon, timestamp, destination FROM   
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


def run():
    """Main entry point; defines and runs the wordcount pipeline.
    """
    known_args, pipeline_options = parse_command_line_args()

    p = beam.Pipeline(options=pipeline_options)

    config = load_config(known_args.config)

    query = create_query(known_args)

    tagged_paths = (p 
        | "ReadAis" >> Source(query)
        | CreateVesselRecords(config['blacklisted_mmsis'])
        | CreateTaggedPaths(config['min_required_positions'])
        )

    anchorages = (p
        | 'ReadAnchorages' >> ReadFromText(known_args.anchorage_path)
        | CreateTaggedAnchorages()
        )

    (tagged_paths
        | CreateInOutEvents(anchorage_entry_dist=config['anchorage_entry_distance_km'], 
                            anchorage_exit_dist=config['anchorage_exit_distance_km'], 
                            anchorages=anchorages)
        | "writeInOutEvents" >> EventSink(table=known_args.output, write_disposition="WRITE_TRUNCATE")
        # WriteToText(known_args.output, 
        #                                     file_name_suffix='.json', 
        #                                     num_shards=(0 if known_args.shard_output else 1))
        )

    result = p.run()
    result.wait_until_finish()

