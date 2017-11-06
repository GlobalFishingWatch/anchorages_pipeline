import os
import pytest
import json
import datetime
import pickle
from anchorages import common
from anchorages.records import is_location_message, has_valid_location
from anchorages.records import VesselRecord
from anchorages.records import VesselLocationRecord


this_dir = os.path.dirname(__file__)
path = os.path.join(this_dir, "example_messages.json")
with open(path) as f:
    example_data = f.read().strip()

examples_msgs = [json.loads(x) for x in example_data.split("\n")]

# Sabotage example message so that we can see some bad messages later
examples_msgs[3]['lat'] = 361

example_records = [VesselRecord.tagged_from_msg(x)[1] for x in examples_msgs if is_location_message(x)]
example_records.append


def test_is_location_message():
    assert [is_location_message(x) for x in examples_msgs] == [1, 0, 1, 1, 0]

def test_has_valid_location():
    assert [has_valid_location(x) for x in examples_msgs if is_location_message(x)] == [True, True, False]

class TestVesselLocationRecord(object):

    def test_create(self):
        (md, obj) = VesselRecord.tagged_from_msg(examples_msgs[0])
        assert obj == VesselLocationRecord(
                    timestamp=datetime.datetime(2016, 1, 1, 5, 20, 13), 
                    location=common.LatLon(lat=55.2189674377, lon=9.2907962799), 
                    destination=None, 
                    speed=15.8999996185)

    def test_pickle(self):
        (md, obj) = VesselRecord.tagged_from_msg(examples_msgs[0])
        assert pickle.loads(pickle.dumps(obj)) == obj





