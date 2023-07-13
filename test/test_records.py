import os
import pytest
import json
import datetime
import pickle
import pytz
from pipe_anchorages import common
from pipe_anchorages.records import is_location_message, has_valid_location
from pipe_anchorages.records import InvalidRecord
from pipe_anchorages.records import VesselRecord
from pipe_anchorages.records import VesselLocationRecord


this_dir = os.path.dirname(__file__)
path = os.path.join(this_dir, "example_messages.json")
with open(path) as f:
    example_data = f.read().strip()

examples_msgs = [json.loads(x) for x in example_data.split("\n")]
for msg in examples_msgs:
    msg['timestamp'] = (datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S.%f UTC')
                        .replace(tzinfo=pytz.utc).timestamp())
# '2016-01-01 05:20:13.000000 UTC' 
# Sabotage example message so that we can see some bad messages later
examples_msgs[3]['lat'] = 361

example_records = [VesselRecord.tagged_from_msg(x)[1] for x in examples_msgs if is_location_message(x)]
example_records.append


def test_date_parsing_records():
    msgs = [{'ident':123,'timestamp':'2021-04-26 06:00:12.0000 UTC'}, {'ident':123,'timestamp':'2021-05-04 12:20:42.798437 UTC'}]
    for msg in msgs:
        msg['timestamp'] = (datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S.%f UTC')
                        .replace(tzinfo=pytz.utc).timestamp())
    assert [InvalidRecord.from_msg(x) for x in msgs] == [
    InvalidRecord(identifier=123,timestamp=datetime.datetime(2021, 4, 26, 6, 0, 12, tzinfo=pytz.UTC)),
    InvalidRecord(identifier=123,timestamp=datetime.datetime(2021, 5, 4, 12, 20, 42, 798437, tzinfo=pytz.UTC))]

def test_is_location_message():
    assert [is_location_message(x) for x in examples_msgs] == [1, 0, 1, 1, 0]

def test_has_valid_location():
    assert [has_valid_location(x) for x in examples_msgs if is_location_message(x)] == [True, True, False]

class TestVesselLocationRecord(object):

    def test_create(self):
        (md, obj) = VesselRecord.tagged_from_msg(examples_msgs[0])
        assert obj == VesselLocationRecord(
                    identifier=111219501,
                    timestamp=datetime.datetime(2016, 1, 1, 5, 20, 13, tzinfo=pytz.UTC), 
                    location=common.LatLon(lat=55.2189674377, lon=9.2907962799), 
                    destination=None, 
                    speed=15.8999996185)

    def test_pickle(self):
        (md, obj) = VesselRecord.tagged_from_msg(examples_msgs[0])
        assert pickle.loads(pickle.dumps(obj)) == obj





