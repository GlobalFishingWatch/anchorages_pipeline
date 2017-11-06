from __future__ import absolute_import, print_function, division

import argparse
import csv
import datetime
import os
import s2sphere
import math
from collections import namedtuple
import logging

from . import common as cmn
from .anchorages import AnchoragePoint
from .distance import distance
from .nearest_port import Port
from .shapefile_to_iso3 import Iso3Finder
from .transforms.source import QuerySource
from .transforms.sink import NamedAnchorageSink

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

Port = namedtuple("Port", ["name", "country", "lat", "lon"])



this_dir = os.path.dirname(__file__)
inf = float("inf")


class NamedAnchoragePoint(namedtuple("NamedAnchoragePoint", 
        AnchoragePoint._fields + ('label', 'sublabel'))):

    __slots__ = ()

    @staticmethod
    def from_msg(msg):
        msg['mean_location'] = cmn.LatLon(msg.pop('lat'), msg.pop('lon'))
        msg['rms_drift_radius'] = msg.pop('drift_radius')
        msg['vessels'] = [None] * msg.pop('unique_stationary_mmsi')
        msg['fishing_vessels'] = [None] * msg.pop('unique_stationary_fishing_mmsi')
        msg['active_mmsi'] = msg.pop('unique_active_mmsi')
        msg['total_mmsi'] = msg.pop('unique_total_mmsi')
        msg['wpi_name'] = Port(msg.pop('wpi_name'), msg.pop('wpi_country'), 
                               msg.pop('wpi_lat'), msg.pop('wpi_lon'))
        msg['geonames_name'] = Port(msg.pop('geonames_name'), msg.pop('geonames_country'), 
                                    msg.pop('geonames_lat'), msg.pop('geonames_lon'))
        msg['label'] = None
        msg['sublabel'] = None
        msg['neighbor_s2ids'] = None
        return NamedAnchoragePoint(**msg)







class AddNamesToAnchorages(beam.PTransform):

    def __init__(self, shapefile_path, override_path):
        self.iso3finder = Iso3Finder(shapefile_path)
        self.override_list = []
        with open(os.path.join(this_dir, override_path)) as csvfile:
            for x in  csv.DictReader(csvfile):
                x['latLon'] = cmn.LatLon(float(x['anchor_lat']), float(x['anchor_lon']))
                x['s2id'] = x['latLon'].S2CellId(scale=cmn.ANCHORAGES_S2_SCALE).to_token()
                self.override_list.append(x)

    def add_best_label(self, anchorage):
        if anchorage.wpi_distance < 4:
            label = anchorage.wpi_name.name
        elif anchorage.geonames_distance < 4:
            label = anchorage.geonames_name.name
        else:
            label = anchorage.top_destination
        map = anchorage._asdict()
        map['label'] = unicode(label)
        map['sublabel'] = None
        return NamedAnchoragePoint(**map)

    def add_iso3(self, named_anchorage):
        iso3 = self.iso3finder.iso3(named_anchorage.mean_location.lat, 
                                    named_anchorage.mean_location.lon)
        if iso3 is None:
            iso3 = "---"
        if iso3 == "CHN":
            named_anchorage = named_anchorage._replace(label=named_anchorage.s2id)
        return named_anchorage._replace(label=u"{},{}".format(named_anchorage.label, iso3))

    def apply_override_list(self, named_anchorage):
        min_dist = inf
        min_row = None
        for row in self.override_list:
            if row['s2id'] == named_anchorage.s2id:
                # If S2id's match, replace with new anchorage, but keep old stats
                sublabel = sublabel=row['sublabel'].strip()
                if not sublabel:
                    sublabel = None
                return named_anchorage._replace(label=row['label'],
                                                sublabel=sublabel,
                                                mean_location=cmn.LatLon(
                                                    lat=row['anchor_lat'],
                                                    lon=row['anchor_lon']))
            dist = distance(named_anchorage.mean_location, row['latLon'])
            if dist < min_dist:
                min_dist = dist
                min_row = row
        if min_dist < 4:
            # Otherwise if any vessels in the list are within 4 km, replace just the labe.
            named_anchorage = named_anchorage._replace(label=min_row['label'])
        return named_anchorage

    def expand(self, anchorages):
        return (anchorages
            | beam.Map(self.add_best_label)
            | beam.Map(self.add_iso3)
            | beam.Map(self.apply_override_list)
            )



class FindUsedS2ids(beam.PTransform):

    def __init__(self, override_path):
        self.override_list = []
        with open(os.path.join(this_dir, override_path)) as csvfile:
            for x in  csv.DictReader(csvfile):
                x['latLon'] = cmn.LatLon(float(x['anchor_lat']), float(x['anchor_lon']))
                x['s2id'] = x['latLon'].S2CellId(scale=cmn.ANCHORAGES_S2_SCALE).to_token()
                self.override_list.append(x)

    def find_used_s2ids(self, named_anchorage):
        for row in self.override_list:
            if row['s2id'] == named_anchorage.s2id:
                yield row['s2id']
                break

    def expand(self, anchorages):
        return anchorages | beam.FlatMap(self.find_used_s2ids)



class CreateOverrideAnchorages(beam.PTransform):

    def __init__(self, override_path, used_s2ids):
        self.override_list = []
        with open(os.path.join(this_dir, override_path)) as csvfile:
            for x in  csv.DictReader(csvfile):
                x['latLon'] = cmn.LatLon(float(x['anchor_lat']), float(x['anchor_lon']))
                x['s2id'] = x['latLon'].S2CellId(scale=cmn.ANCHORAGES_S2_SCALE).to_token()
                self.override_list.append(x)
        self.used_s2ids = used_s2ids

    def create_override_anchorages(self, dummy, used_s2ids):
        used_s2ids = set(used_s2ids)
        for row in self.override_list:
            if row['s2id'] not in used_s2ids:
                yield NamedAnchoragePoint(
                        mean_location = row['latLon'],
                        total_visits = None, 
                        vessels = frozenset([]),
                        fishing_vessels = frozenset([]),
                        rms_drift_radius =  None,    
                        top_destination = None,
                        s2id = row['s2id'],
                        neighbor_s2ids = None,
                        active_mmsi = None,
                        total_mmsi = None,
                        stationary_mmsi_days = None,
                        stationary_fishing_mmsi_days = None,
                        active_mmsi_days = None,
                        wpi_name = Port(None, None, None, None),
                        wpi_distance = None,
                        geonames_name = Port(None, None, None, None),
                        geonames_distance = None,
                        label=row['label'],
                        sublabel=row['sublabel']
                        )

    def expand(self, p):
        return p | beam.Create([None]) | beam.FlatMap(self.create_override_anchorages, self.used_s2ids)



def parse_command_line_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--name', required=True, 
                        help='Name to prefix output and job name if not otherwise specified')
    # TODO: Replace
    parser.add_argument('--output-table', 
                        help='Output table to write results to.')
    parser.add_argument('--input-table', default='pipeline_classify_p_p429_resampling_2',
                        help='Input table to pull data from')
    parser.add_argument('--override-path', default='anchorage_overrides.csv')
    parser.add_argument('--shapefile-path', default=os.path.join(this_dir, 'EEZ/EEZ_land_v2_201410.shp'),
                        help="path to configuration file")

    known_args, pipeline_args = parser.parse_known_args()

    if known_args.output_table is None:
        known_args.output_table = 'machine_learning_dev_ttl_30d.anchorages_{}'.format(known_args.name)

    cmn.add_pipeline_defaults(pipeline_args, known_args.name)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    cmn.check_that_pipeline_args_consumed(pipeline_options)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    return known_args, pipeline_options


def create_query(args):
    template = """
    SELECT * FROM [world-fishing-827:{table}] 
    """
    return template.format(table=args.input_table)



def run():
    known_args, pipeline_options = parse_command_line_args()

    p = beam.Pipeline(options=pipeline_options)

    source = p | QuerySource(create_query(known_args))

    existiting_anchorages = (source
        | beam.Map(NamedAnchoragePoint.from_msg)
        | AddNamesToAnchorages(known_args.shapefile_path, known_args.override_path)
    )


    used_s2ids = beam.pvalue.AsList(existiting_anchorages | FindUsedS2ids(known_args.override_path))

    new_anchorages = p | CreateOverrideAnchorages(known_args.override_path, used_s2ids)

    named_anchorages = (existiting_anchorages, new_anchorages) | beam.Flatten()

    (named_anchorages | NamedAnchorageSink(table=known_args.output_table, 
                                      write_disposition="WRITE_TRUNCATE")
    )

    result = p.run()
    result.wait_until_finish()

