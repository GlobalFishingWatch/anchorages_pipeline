import datetime
import struct
from typing import NamedTuple

import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to

from pipe_anchorages.transforms.shrink_group_by import ShrinkGroupBy

# Beam's local runner, named explicitly: DirectRunner would fall back to other runners for
# pipelines Prism doesn't support.
RUNNER = "PrismRunner"

DT = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
DT_US = 1767268800_000000


class SimpleRecord(NamedTuple):
    identifier: str
    position: int


class MultiKeyRecord(NamedTuple):
    ssvid: str
    vessel_id: str
    seg_id: str
    timestamp: datetime.datetime
    speed: float


class Point(NamedTuple):
    lat: float
    lon: float


class Label(NamedTuple):
    name: str
    weight: float


class PositionRecord(NamedTuple):
    identifier: str
    point: Point
    label: Label


def assert_groups(records, shrink_group_by, expected):
    """Runs `shrink_group_by` over `records` and checks the groups it returns."""
    with beam.Pipeline(runner=RUNNER) as pipeline:
        grouped = pipeline | beam.Create(records) | shrink_group_by
        assert_that(grouped, equal_to(expected))


def test_records_are_grouped_by_their_key():
    records = [SimpleRecord("a", 1), SimpleRecord("b", 2), SimpleRecord("a", 3)]

    assert_groups(
        records,
        ShrinkGroupBy("identifier", record_type=SimpleRecord),
        expected=[("a", [records[0], records[2]]), ("b", [records[1]])],
    )


def test_pipeline_with_multi_keys():
    rec1 = MultiKeyRecord("111", "v-1", "s-1", DT, 5.0)
    rec2 = MultiKeyRecord("111", "v-1", "s-2", DT, 6.0)
    rec3 = MultiKeyRecord("222", "v-2", "s-3", DT, 7.0)

    assert_groups(
        [rec1, rec2, rec3],
        ShrinkGroupBy("ssvid", "vessel_id", record_type=MultiKeyRecord),
        expected=[
            (("111", "v-1"), [rec1, rec2]),
            (("222", "v-2"), [rec3]),
        ],
    )


def test_shrunken_value_omits_key_fields():
    rec = SimpleRecord("a", 42)
    shrink_group_by = ShrinkGroupBy("identifier", record_type=SimpleRecord)

    key, shrunken = shrink_group_by.shrink(rec)

    assert key == "a"
    assert shrunken == (42,)

    _, rebuilt = shrink_group_by.unshrink((key, [shrunken]))
    assert rebuilt == [rec]


def test_multi_key_omits_all_key_fields_and_converts_datetimes():
    rec = MultiKeyRecord("111", "v-1", "s-1", DT, 10.5)
    shrink_group_by = ShrinkGroupBy("ssvid", "vessel_id", record_type=MultiKeyRecord)

    key, shrunken = shrink_group_by.shrink(rec)

    assert key == ("111", "v-1")
    assert shrunken == ("s-1", DT_US, 10.5)

    _, rebuilt = shrink_group_by.unshrink((key, [shrunken]))
    assert rebuilt == [rec]


def test_computed_keys_go_after_the_fields_and_keep_their_source():
    rec = MultiKeyRecord("111", "v-1", "s-1", DT, 10.5)
    shrink_group_by = ShrinkGroupBy(
        "ssvid", record_type=MultiKeyRecord, date=lambda r: str(r.timestamp.date()),
    )

    key, shrunken = shrink_group_by.shrink(rec)

    assert key == ("111", "2026-01-01")
    assert shrunken == ("v-1", "s-1", DT_US, 10.5)

    _, rebuilt = shrink_group_by.unshrink((key, [shrunken]))
    assert rebuilt == [rec]


def test_a_single_computed_key_is_not_a_tuple():
    rec = SimpleRecord("a", 42)
    shrink_group_by = ShrinkGroupBy(record_type=SimpleRecord, parity=lambda r: r.position % 2)

    key, shrunken = shrink_group_by.shrink(rec)

    assert key == 0
    assert shrunken == ("a", 42)

    _, rebuilt = shrink_group_by.unshrink((key, [shrunken]))
    assert rebuilt == [rec]


def test_requires_namedtuple():
    class PlainClass:
        identifier: str
        position: int

    with pytest.raises(TypeError, match="must be a NamedTuple with _fields"):
        ShrinkGroupBy("identifier", record_type=PlainClass)


def test_validates_keys():
    with pytest.raises(ValueError, match="Key 'nonexistent' is not declared"):
        ShrinkGroupBy("nonexistent", record_type=SimpleRecord)


def test_requires_a_key():
    with pytest.raises(ValueError, match="At least one field or computed key is required"):
        ShrinkGroupBy(record_type=SimpleRecord)


def test_requires_a_record_type():
    with pytest.raises(TypeError, match="record_type"):
        ShrinkGroupBy("identifier")


def test_is_labeled_like_group_by():
    shrink_group_by = ShrinkGroupBy(
        "ssvid", record_type=MultiKeyRecord, elements="Positions", date=lambda r: r.timestamp,
    )

    assert shrink_group_by.label == "GroupPositionsBySsvidAndDate"


def test_namedtuple_fields_of_floats_are_packed_into_bytes():
    rec = PositionRecord("a", Point(55.2189674377, 9.2907962799), Label("x", 1.0))
    shrink_group_by = ShrinkGroupBy("identifier", record_type=PositionRecord)

    key, shrunken = shrink_group_by.shrink(rec)

    assert shrunken[0] == struct.pack("<dd", 55.2189674377, 9.2907962799)
    assert shrunken[1] == Label("x", 1.0)

    _, rebuilt = shrink_group_by.unshrink((key, [shrunken]))
    assert rebuilt == [rec]
    assert type(rebuilt[0].point) is Point


def test_pipeline_with_packed_fields():
    records = [
        PositionRecord("a", Point(1.5, 2.5), Label("x", 1.0)),
        PositionRecord("a", Point(3.5, 4.5), Label("y", 2.0)),
    ]

    assert_groups(
        records,
        ShrinkGroupBy("identifier", record_type=PositionRecord),
        expected=[("a", records)],
    )


def test_datetimes_round_trip_exactly_as_integer_microseconds():
    dt = datetime.datetime(2026, 10, 1, 1, 2, 3, 456789, tzinfo=datetime.timezone.utc)
    rec = MultiKeyRecord("111", "v-1", "s-1", dt, 10.5)
    shrink_group_by = ShrinkGroupBy("ssvid", "vessel_id", record_type=MultiKeyRecord)

    key, shrunken = shrink_group_by.shrink(rec)

    assert shrunken[1] == 1790816523_456789
    _, rebuilt = shrink_group_by.unshrink((key, [shrunken]))
    assert rebuilt[0].timestamp == dt
