from apache_beam import PTransform
from apache_beam import Filter
from apache_beam import Map

import datetime as dt


class Cleanup(PTransform):

    def expand(self, xs):
        def is_valid(x):
            return (is_location_message(x) and has_valid_location(x)) or has_destination(x)

        def parse_timestamp(x):
            raw_timestamp = x['timestamp']
            timestamp = dt.datetime.strptime(raw_timestamp, "%Y-%m-%d %H:%M:%S.%f %Z")

            x['timestamp'] = timestamp
            return x

        return xs | Filter(is_valid) | Map(parse_timestamp)