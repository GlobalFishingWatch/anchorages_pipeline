
from __future__ import absolute_import, print_function, division

import datetime
from collections import namedtuple
import s2sphere
import yaml

from .records import VesselRecord
from .records import InvalidRecord
from .records import VesselLocationRecord

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


# Around (0.5 km)^2
ANCHORAGES_S2_SCALE = 14
# Around (16 km)^2
VISITS_S2_SCALE = 9

approx_visit_cell_size = 2.0 ** (13 - VISITS_S2_SCALE) 
VISIT_SAFETY_FACTOR = 2.0 # Extra margin factor to ensure we don't miss ports


class CreateVesselRecords(beam.PTransform):

    def __init__(self, blacklisted_vessel_ids, **defaults):
        self.blacklisted_vessel_ids = set(blacklisted_vessel_ids)
        self.defaults = defaults

    def is_valid(self, item):
        vessel_id, rcd = item
        assert isinstance(rcd, VesselRecord), type(rcd)
        return (not isinstance(rcd, InvalidRecord) and 
                isinstance(vessel_id, basestring) and
                (vessel_id not in self.blacklisted_vessel_ids)) 

    def add_defaults(self, x):
        for k, v in self.defaults.items():
            if k not in x:
                x[k] = v
        return x

    def expand(self, ais_source):
        return (ais_source
            | beam.Map(self.add_defaults)
            | beam.Map(VesselRecord.tagged_from_msg)
            | beam.Filter(self.is_valid)
        )


class CreateTaggedRecords(beam.PTransform):

    def __init__(self, min_required_positions):
        self.min_required_positions = min_required_positions
        self.FIVE_MINUTES = datetime.timedelta(minutes=5)

    def order_by_timestamp(self, item):
        vessel_id, records = item
        records = list(records)
        records.sort(key=lambda x: x.timestamp)
        return vessel_id, records

    def dedup_by_timestamp(self, item):
        key, source = item
        seen = set()
        sink = []
        for x in source:
            if x.timestamp not in seen:
                sink.append(x)
                seen.add(x.timestamp)
        return (key, sink)

    def long_enough(self, item):
        vessel_id, records = item
        return len(records) >= self.min_required_positions

    def thin_records(self, item):
        vessel_id, records = item
        last_timestamp = datetime.datetime(datetime.MINYEAR, 1, 1)
        thinned = []
        for rcd in records:
            if (rcd.timestamp - last_timestamp) >= self.FIVE_MINUTES:
                last_timestamp = rcd.timestamp
                thinned.append(rcd)
        return vessel_id, thinned

    def tag_records(self, item):
        vessel_id, records = item
        dest = ''
        tagged = []
        for rcd in records:
            if isinstance(rcd, VesselInfoRecord):
                # TODO: normalize here rather than later. And cache normalization in dictionary
                dest = rcd.destination
            elif isinstance(rcd, VesselLocationRecord):
                tagged.append(rcd._replace(destination=dest))
            else:
                raise RuntimeError('unknown type {}'.format(type(rcd)))
        return (vessel_id, tagged)

    def expand(self, vessel_records):
        return (vessel_records
            | beam.GroupByKey()
            | beam.Map(self.order_by_timestamp)
            | beam.Map(self.dedup_by_timestamp)
            | beam.Filter(self.long_enough)
            | beam.Map(self.thin_records)
            | beam.Map(self.tag_records)
            )



def load_config(path):
    with open(path) as f:
        config = yaml.load(f.read())

    anchorage_visit_max_distance = max(config['anchorage_entry_distance_km'],
                                       config['anchorage_exit_distance_km'])

    # Ensure that S2 Cell sizes are large enough that we don't miss ports
    assert (anchorage_visit_max_distance * VISIT_SAFETY_FACTOR < 2 * approx_visit_cell_size)

    return config


def mean(iterable):
    n = 0
    total = 0.0
    for x in iterable:
        total += x
        n += 1
    return (total / n) if n else 0


class LatLon(
    namedtuple("LatLon", ["lat", "lon"])):

    __slots__ = ()

    def S2CellId(self, scale=None):
        ll = s2sphere.LatLng.from_degrees(self.lat, self.lon)
        cellid = s2sphere.CellId.from_lat_lng(ll)
        if scale is not None:
            cellid = cellid.parent(scale)
        return cellid


def add_pipeline_defaults(pipeline_args, name):

    defaults = {
        '--project' : 'world-fishing-827',
        '--staging_location' : 'gs://machine-learning-dev-ttl-30d/anchorages/{}/output/staging'.format(name),
        '--temp_location' : 'gs://machine-learning-dev-ttl-30d/anchorages/temp',
        '--setup_file' : './setup.py',
        '--runner': 'DataflowRunner',
        '--max_num_workers' : '200',
        '--disk_size_gb' : '100',
        '--job_name': name,
    }

    for name, value in defaults.items():
        if name not in pipeline_args:
            pipeline_args.extend((name, value))


def check_that_pipeline_args_consumed(pipeline):
    options = pipeline.get_all_options(drop_default=True)

    # Some options get translated on the way in (should be a better way to do this...)
    translations = {'--worker_machine_type' : '--machine_type'}
    flags = [translations.get(x, x) for x in pipeline._flags]

    dash_flags = [x for x in flags if x.startswith('-') and x.replace('-', '') not in options
                    and x != '--experiments=shuffle_mode=service']
    if dash_flags:
        print(options)
        print(dash_flags)
        raise ValueError('illegal options specified:\n    {}'.format('\n    '.join(dash_flags)))





