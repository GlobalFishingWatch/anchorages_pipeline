import yaml
from importlib.resources import files

from pipe_anchorages.assets import config as config_pkg
from pipe_anchorages.port_info_finder import PortInfoFinder, normalize_label


default_config_path = files(config_pkg).joinpath("name_anchorages_cfg.yaml")

with open(default_config_path) as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)


def test_instantiation():
    PortInfoFinder.from_config(config)


def test_normalize():
    assert normalize_label("abc_123") == "ABC_123"
