import json as ujson
import apache_beam as beam
import datetime


epoch = datetime.datetime.fromtimestamp(0, datetime.UTC)


def datetime_to_s(x):
    return (x - epoch).total_seconds()


_datetime_to_s = datetime_to_s


def s_to_datetime(x):
    return epoch + datetime.timedelta(seconds=x)


_s_to_datetime = s_to_datetime


class NamedtupleCoder(beam.coders.Coder):
    """A coder used for reading and writing nametuples to/from json"""

    # Overide target with actual target
    target = None
    # Overide time_fields with sequence of field names containing datetime instances
    time_fields = []

    @classmethod
    def _encode(cls, value):
        replacements = {x: _datetime_to_s(getattr(value, x)) for x in cls.time_fields}
        return value._replace(**replacements)

    @classmethod
    def encode(cls, value):
        return ujson.dumps(cls._encode(value))

    @classmethod
    def _decode(cls, value):
        replacements = {x: _s_to_datetime(getattr(value, x)) for x in cls.time_fields}
        return value._replace(**replacements)

    @classmethod
    def decode(cls, value):
        return cls._decode(cls.target(*ujson.loads(value)))

    def is_deterministic(self):
        return True

    @classmethod
    def register(cls):
        beam.coders.registry.register_coder(cls.target, cls)
