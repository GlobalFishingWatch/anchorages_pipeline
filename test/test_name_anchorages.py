# -*- coding: utf-8 -*-
import os
import yaml
import pytest
from pipe_anchorages import common as cmn
from pipe_anchorages import name_anchorages_pipeline as name_anchorages

config = yaml.load("""
label_distance_km: 4.0
sublabel_distance_km: 1.0
override_path: anchorage_overrides.csv
port_list_paths:
    - WPI_ports.csv
    - geonames_1000.csv
    """, Loader=yaml.SafeLoader)


class TestNameAnchorages(object):

    def test_normalize(self):
        normalize = name_anchorages.normalize_label
        assert normalize(u'Спецморнефтепорт'.encode('utf-8')) == "SPETSMORNEFTEPORT"
        assert normalize(u'Tromsø'.encode('utf-8')) == "TROMSO"


    def test_finder(self):
        finder = name_anchorages.PortInfoFinder.from_config(config)
        assert finder.find(cmn.LatLon(62.69251317945518, 6.640516316894406))[0].label == "Midsund"


