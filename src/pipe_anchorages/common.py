from __future__ import absolute_import, division, print_function

from collections import namedtuple

import apache_beam as beam
import s2sphere
import six

from .records import InvalidRecord, VesselInfoRecord, VesselLocationRecord, VesselRecord

# Around (0.5 km)^2
ANCHORAGES_S2_SCALE = 14
# Around (16 km)^2
VISITS_S2_SCALE = 9

approx_visit_cell_size = 2.0 ** (13 - VISITS_S2_SCALE)
VISIT_SAFETY_FACTOR = 2.0  # Extra margin factor to ensure we don't miss ports


class CreateVesselRecords(beam.PTransform):
    def __init__(self, **defaults):
        self.defaults = defaults

    def is_valid(self, item):
        ident, rcd = item
        assert isinstance(rcd, VesselRecord), type(rcd)
        return not isinstance(rcd, InvalidRecord) and isinstance(ident, six.string_types)

    def add_defaults(self, x):
        for k, v in self.defaults.items():
            if k not in x:
                x[k] = v
        return x

    def expand(self, ais_source):
        return (
            ais_source
            | beam.Map(self.add_defaults)
            | beam.Map(VesselRecord.tagged_from_msg)
            | beam.Filter(self.is_valid)
        )


class CreateTaggedRecordsByDay(beam.PTransform):
    def add_date_to_key(self, item):
        identity, value = item
        return (identity, str(value.timestamp.date())), value

    def order_by_timestamp(self, item):
        key, records = item
        records = sorted(records, key=lambda x: (x.timestamp, x.speed, x.location))
        return key, records

    def dedup_by_timestamp(self, item):
        key, source = item
        seen = set()
        sink = []
        for x in sorted(source, key=lambda x: (x.timestamp, x.speed, x.location)):
            if x.timestamp not in seen:
                sink.append(x)
                seen.add(x.timestamp)
        return (key, sink)

    def tag_records(self, item):
        ident, records = item
        dest = ""
        tagged = []
        for rcd in records:
            if isinstance(rcd, VesselInfoRecord):
                dest = rcd.destination
            elif isinstance(rcd, VesselLocationRecord):
                tagged.append(rcd._replace(destination=dest))
            else:
                raise RuntimeError("unknown type {}".format(type(rcd)))
        return (ident, tagged)

    def expand(self, vessel_records):
        return (
            vessel_records
            | beam.Map(self.add_date_to_key)
            | beam.GroupByKey()
            | beam.Map(self.order_by_timestamp)
            | beam.Map(self.dedup_by_timestamp)
            | beam.Map(self.tag_records)
        )


class LatLon(namedtuple("LatLon", ["lat", "lon"])):

    __slots__ = ()

    def S2CellId(self, scale=None):
        ll = s2sphere.LatLng.from_degrees(self.lat, self.lon)
        cellid = s2sphere.CellId.from_lat_lng(ll)
        if scale is not None:
            cellid = cellid.parent(scale)
        return cellid
