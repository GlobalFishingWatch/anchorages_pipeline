import os
from collections import namedtuple
import yaml

from pipe_anchorages.common import LatLon
from pipe_anchorages.distance import distance
from pipe_anchorages.port_info_finder import PortInfoFinder, normalize_label
from pipe_anchorages import dirnames

this_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(this_dir)

default_config_path = os.path.join(dirnames.parent_dir, 'name_anchorages_cfg.yaml')
with open(default_config_path) as f:
    config = yaml.load(f)

def test_instantiation():
    finder = PortInfoFinder.from_config(config)


def test_normalize():
    assert normalize_label('abc_123') == 'ABC_123'

