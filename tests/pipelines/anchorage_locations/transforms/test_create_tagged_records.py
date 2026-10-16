import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to

from pipe_anchorages.pipelines.anchorage_locations.transforms.create_tagged_records import (
    CreateTaggedRecords,
)

from .factories import info, location


def test_order_by_timestamp_sorts_by_timestamp_then_speed():
    records = [location("1", 10), location("1", 0, speed=2.0), location("1", 0, speed=1.0)]

    _, ordered = CreateTaggedRecords(1).order_by_timestamp(("1", records))

    assert [(r.timestamp, r.speed) for r in ordered] == [
        (records[2].timestamp, 1.0), (records[1].timestamp, 2.0), (records[0].timestamp, 0.0)
    ]


def test_dedup_by_timestamp_keeps_the_first_record_in_sort_order():
    slow, fast = location("1", 0, speed=1.0), location("1", 0, speed=5.0)

    _, deduped = CreateTaggedRecords(1).dedup_by_timestamp(("1", [fast, slow, location("1", 1)]))

    assert deduped == [slow, location("1", 1)]


@pytest.mark.parametrize("n, expected", [(2, False), (3, True), (4, True)])
def test_long_enough_needs_min_required_positions(n, expected):
    records = [location("1", i) for i in range(n)]

    assert CreateTaggedRecords(3).long_enough(("1", records)) is expected


def test_thin_records_keeps_positions_at_least_five_minutes_apart():
    records = [location("1", m) for m in (0, 2, 4, 5, 9, 10, 16)]

    _, thinned = CreateTaggedRecords(1).thin_records(("1", records))

    assert [r.timestamp for r in thinned] == [records[i].timestamp for i in (0, 3, 5, 6)]


def test_thin_records_accepts_timezone_aware_timestamps():
    """Regression: thin_records compared aware timestamps with a naive datetime and failed."""
    record = location("1", 0)
    assert record.timestamp.tzinfo is not None

    assert CreateTaggedRecords(1).thin_records(("1", [record])) == ("1", [record])


def test_thin_records_is_skipped_when_thin_is_false():
    records = [location("1", m) for m in (0, 1, 2)]

    assert CreateTaggedRecords(1, thin=False).thin_records(("1", records)) == ("1", records)


def test_tag_records_sets_the_latest_destination_and_drops_info_records():
    records = [
        location("1", 0), info("1", 1, "BUENOS AIRES"), location("1", 2),
        info("1", 3, "MONTEVIDEO"), location("1", 4),
    ]

    _, tagged = CreateTaggedRecords(1).tag_records(("1", records))

    assert [r.destination for r in tagged] == ["", "BUENOS AIRES", "MONTEVIDEO"]


def test_tag_records_rejects_unknown_record_types():
    with pytest.raises(RuntimeError, match="unknown type"):
        CreateTaggedRecords(1).tag_records(("1", [object()]))


def test_expand_builds_a_sorted_deduped_thinned_track_per_vessel(pipeline):
    # The one Beam test of these transforms: the order of the steps only exists in expand.
    keyed = (
        [("1", location("1", m)) for m in (10, 0, 0, 5)]
        # Too few positions.
        + [("2", location("2", 0))]
        # Documents current behavior: the minimum is checked before thinning, so a vessel can
        # end up with fewer positions than required.
        + [("3", location("3", m)) for m in (0, 1, 2)]
    )

    with pipeline as p:
        tracks = p | beam.Create(keyed) | CreateTaggedRecords(3)

        assert_that(tracks, equal_to([
            ("1", [location("1", m, destination="") for m in (0, 5, 10)]),
            ("3", [location("3", 0, destination="")]),
        ]))
