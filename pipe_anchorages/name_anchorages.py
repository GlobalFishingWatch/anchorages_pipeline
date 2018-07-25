from __future__ import absolute_import, print_function, division

import argparse
import datetime
import os
import s2sphere
import math
from collections import namedtuple
import logging
import yaml

from . import common as cmn
from .find_anchorage_points import AnchoragePoint
from .nearest_port import Port
from .shapefile_to_iso3 import get_iso3_finder
from .transforms.source import QuerySource
from .transforms.sink import NamedAnchorageSink
from .get_override_list import get_override_list
from .port_info_finder import PortInfoFinder
from .port_info_finder import mangled_path
from .port_info_finder import normalize_label

from . import dirnames
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


inf = float("inf")


class NamedAnchoragePoint(namedtuple("NamedAnchoragePoint", 
        AnchoragePoint._fields + ('label', 'sublabel', 'iso3', 'label_source'))):

    __slots__ = ()

    @staticmethod
    def from_msg(msg):
        msg['mean_location'] = cmn.LatLon(msg.pop('lat'), msg.pop('lon'))
        msg['rms_drift_radius'] = msg.pop('drift_radius')
        msg['vessels'] = [None] * msg.pop('unique_stationary_vessel_id')
        msg['fishing_vessels'] = [None] * msg.pop('unique_stationary_fishing_vessel_id')
        msg['active_vessel_id'] = msg.pop('unique_active_vessel_id')
        msg['total_vessel_id'] = msg.pop('unique_total_vessel_id')
        msg['label'] = None
        msg['sublabel'] = None
        msg['iso3'] = None
        msg['label_source'] = None
        msg['neighbor_s2ids'] = None

        msg.pop('geonames_distance')
        msg.pop('wpi_distance')
        msg.pop('geonames_name')
        msg.pop('geonames_country')
        msg.pop('geonames_lon')
        msg.pop('geonames_lat')
        msg.pop('wpi_name')
        msg.pop('wpi_country')
        msg.pop('wpi_lon')
        msg.pop('wpi_lat')


        return NamedAnchoragePoint(**msg)







class AddNamesToAnchorages(beam.PTransform):

    _port_info_finder = None
    _iso3_finder = None


    def __init__(self, shapefile_path, config):
        self.config = config
        self.shapefile_path = shapefile_path 

    @property
    def port_info_finder(self):
        if self._port_info_finder is None:
            self._port_info_finder = PortInfoFinder.from_config(self.config)
        return self._port_info_finder

    def add_best_label(self, anchorage):
        port_info, source = self.port_info_finder.find(anchorage.mean_location)
        if port_info is None:
            port_info = Port(iso3='', label=anchorage.top_destination, sublabel='', lat=None, lon=None)
            source = 'top_destination' 
        map = anchorage._asdict()
        map['label'] = normalize_label(port_info.label)
        map['sublabel'] = normalize_label(port_info.sublabel)
        map['iso3'] = normalize_label(port_info.iso3)
        map['label_source'] = source
        return NamedAnchoragePoint(**map)

    def add_iso3(self, named_anchorage):
        if named_anchorage.iso3 is None:
            finder = get_iso3_finder(mangled_path(self.shapefile_path, 'EEZ'))
            iso3 = finder.iso3(named_anchorage.mean_location.lat, 
                               named_anchorage.mean_location.lon)
            if iso3 is None:
                iso3 = "---"
        else:
            iso3 = named_anchorage.iso3
        if iso3 == "CHN":
            named_anchorage = named_anchorage._replace(label=named_anchorage.s2id)
        return named_anchorage._replace(label=u"{},{}".format(named_anchorage.label, iso3),
                                         iso3=iso3)

    def expand(self, anchorages):
        return (anchorages
            | beam.Map(self.add_best_label)
            | beam.Map(self.add_iso3)
            )



class FindUsedS2ids(beam.PTransform):

    _override_list = None

    def __init__(self, override_path):
        self.override_path = override_path  

    def find_used_s2ids(self, named_anchorage):
        for row in get_override_list(mangled_path(self.override_path, 'port_lists')):
            if row['s2id'] == named_anchorage.s2id:
                yield row['s2id']
                break

    def expand(self, anchorages):
        return anchorages | beam.FlatMap(self.find_used_s2ids)



class CreateOverrideAnchorages(beam.PTransform):

    _override_list = None

    def __init__(self, override_path, used_s2ids):
        self.override_path = override_path
        self.used_s2ids = used_s2ids

    def create_override_anchorages(self, dummy, used_s2ids):
        used_s2ids = set(used_s2ids)
        for row in get_override_list(mangled_path(self.override_path, 'port_lists')):
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
                        active_vessel_id = None,
                        total_vessel_id = None,
                        stationary_vessel_id_days = None,
                        stationary_fishing_vessel_id_days = None,
                        active_vessel_id_days = None,
                        label=u"{},{}".format(normalize_label(row['label']), row['iso3']),
                        sublabel=normalize_label(row['sublabel']),
                        label_source=os.path.splitext(os.path.basename(self.override_path))[0],
                        iso3=row['iso3'],
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
    parser.add_argument('--input-table', required=True,
                        help='Input anchorage table to pull data from')
    parser.add_argument('--config-path', required=True)
    parser.add_argument('--shapefile-path', default='EEZ_land_v2_201410.shp',
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

    with open(known_args.config_path) as f:
        config = yaml.load(f)

    existing_anchorages = (source
        | beam.Map(NamedAnchoragePoint.from_msg)
        | AddNamesToAnchorages(known_args.shapefile_path, config)
    )

    used_s2ids = beam.pvalue.AsList(existing_anchorages | FindUsedS2ids(config['override_path']))

    new_anchorages = p | CreateOverrideAnchorages(config['override_path'], used_s2ids)

    named_anchorages = (existing_anchorages, new_anchorages) | beam.Flatten()

    (named_anchorages | NamedAnchorageSink(table=known_args.output_table, 
                                      write_disposition="WRITE_TRUNCATE")
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
