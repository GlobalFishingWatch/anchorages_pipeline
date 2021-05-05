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
from .options.name_anchorage_options import NameAnchorageOptions

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import PipelineState

inf = float("inf")





class NamedAnchoragePoint(namedtuple("NamedAnchoragePoint", 
        AnchoragePoint._fields + ('label', 'sublabel', 'iso3', 'label_source'))):

    __slots__ = ()

    @staticmethod
    def from_msg(msg):
        msg['mean_location'] = cmn.LatLon(msg.pop('lat'), msg.pop('lon'))
        msg['rms_drift_radius'] = msg.pop('drift_radius')
        msg['vessels'] = [None] * msg.pop('unique_stationary_ssvid')
        msg['fishing_vessels'] = [None] * msg.pop('unique_stationary_fishing_ssvid')
        msg['active_ssvids'] = msg.pop('unique_active_ssvid')
        msg['total_ssvids'] = msg.pop('unique_total_ssvid')
        msg['stationary_fishing_ssvid_days'] = msg.pop('stationary_fishing_ssvid_days')
        msg['stationary_ssvid_days'] = msg.pop('stationary_ssvid_days')
        msg['active_ssvid_days'] = msg.pop('active_ssvid_days')
        msg['label'] = None
        msg['sublabel'] = None
        msg['iso3'] = None
        msg['label_source'] = None
        msg['neighbor_s2ids'] = None

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
            port_info = Port(iso3='', label=anchorage.top_destination, 
                             sublabel='', lat=None, lon=None)
            source = 'top_destination' 
        map = anchorage._asdict()
        map['label'] = normalize_label(port_info.label)
        if map['label'] in ('', None):
            map['label'] = map['s2id']
        map['sublabel'] = normalize_label(port_info.sublabel)
        map['iso3'] = normalize_label(port_info.iso3)
        map['label_source'] = source
        return NamedAnchoragePoint(**map)

    def add_iso3(self, named_anchorage):
        if named_anchorage.iso3 is None:
            finder = get_iso3_finder(mangled_path(self.shapefile_path, 'EEZ'))
            iso3 = finder.iso3(named_anchorage.mean_location.lat, 
                               named_anchorage.mean_location.lon)
            if iso3 in (None, '-'):
                iso3 = None
        else:
            iso3 = named_anchorage.iso3
        # If the anchorage is in China, just use the s2id as the anchorage ID because
        # unless from anchorage overrides, because Chinese anchorage IDs are so unreliable.
        if iso3 == "CHN" and named_anchorage.label_source != 'anchorage_overrides':
            named_anchorage = named_anchorage._replace(label=named_anchorage.s2id,
                                                       label_source='china_s2id_override')
        return named_anchorage._replace(iso3=iso3)

    def expand(self, anchorages):
        return (anchorages
            | beam.Map(self.add_best_label)
            | beam.Map(self.add_iso3)
            )



class FindUsedS2ids(beam.PTransform):

    _override_list = None

    def __init__(self, override_path):
        self.override_path = override_path  
        self.s2ids_in_overrides = set(row['s2id'] for row in 
            get_override_list(mangled_path(self.override_path, 'port_lists')))

    def find_used_s2ids(self, named_anchorage):
        if named_anchorage.s2id in self.s2ids_in_overrides:
            yield named_anchorage.s2id

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
                        active_ssvids = None,
                        total_ssvids = None,
                        stationary_ssvid_days = None,
                        stationary_fishing_ssvid_days = None,
                        active_ssvid_days = None,
                        label=normalize_label(row['label']),
                        sublabel=normalize_label(row['sublabel']),
                        label_source=os.path.splitext(os.path.basename(self.override_path))[0],
                        iso3=row['iso3'],
                        )

    def expand(self, p):
        return p | beam.Create([None]) | beam.FlatMap(self.create_override_anchorages, self.used_s2ids)






def create_query(args):
    template = """
    SELECT * FROM `{table}`
    """
    return template.format(table=args.input_table)



def run(options):
    known_args = options.view_as(NameAnchorageOptions)
    cloud_options = options.view_as(GoogleCloudOptions)

    p = beam.Pipeline(options=options)

    source = p | QuerySource(create_query(known_args), use_standard_sql=True)

    with open(known_args.config) as f:
        config = yaml.load(f)

    existing_anchorages = (source
        | beam.Map(NamedAnchoragePoint.from_msg)
        | AddNamesToAnchorages(known_args.shapefile, config)
    )

    used_s2ids = beam.pvalue.AsList(existing_anchorages | FindUsedS2ids(config['override_path']))

    new_anchorages = p | CreateOverrideAnchorages(config['override_path'], used_s2ids)

    named_anchorages = ((existing_anchorages, new_anchorages) 
        | beam.Flatten()
        | beam.Filter(lambda x: not (x.label == 'REMOVE' and x.label_source == 'anchorage_overrides'))
    )


    (named_anchorages | NamedAnchorageSink(table=known_args.output_table, 
                                      write_disposition="WRITE_TRUNCATE")
    )

    result = p.run()

    success_states = set([PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN, PipelineState.PENDING])

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
