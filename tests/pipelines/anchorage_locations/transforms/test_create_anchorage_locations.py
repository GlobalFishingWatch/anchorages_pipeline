import collections
import datetime

import pytest

from pipe_anchorages import common as cmn
from pipe_anchorages.pipelines.anchorage_locations.transforms.create_anchorage_locations import (
    CreateAnchorageLocations,
)

from .factories import at, location

LAT, LON = -34.6, -58.4
FAR_LAT = -34.55  # About 5.5 km north: beyond max_distance.
MIN_DURATION = datetime.timedelta(minutes=60)
MAX_DISTANCE_KM = 0.5


def transform(min_unique_vessels=1, fishing_vessels=()):
    return CreateAnchorageLocations(
        MIN_DURATION, MAX_DISTANCE_KM, min_unique_vessels, fishing_vessels
    )


def stay(ident, start, minutes, lat=LAT, lon=LON, destination=""):
    """Positions every 10 minutes at (lat, lon), from `start` for `minutes`, inclusive.

    Destinations default to "", as CreateTaggedRecords sets them.
    """
    return [
        location(ident, m, lat=lat, lon=lon, destination=destination)
        for m in range(start, start + minutes + 1, 10)
    ]


def stay_then_leave(ident, minutes, **kwargs):
    leave = location(ident, minutes + 10, lat=FAR_LAT, destination=kwargs.get("destination", ""))
    return stay(ident, 0, minutes, **kwargs) + [leave]


def minutes_of(records):
    return [int((r.timestamp - at(0)).total_seconds() // 60) for r in records]


def test_split_on_movement_turns_a_long_stay_into_a_stationary_period():
    track = stay_then_leave("1", 90)

    _, split = transform().split_on_movement(("1", track))

    (period,) = split.stationary_periods
    assert period.location == cmn.LatLon(LAT, LON)
    assert period.start_time == at(0)
    assert period.duration == datetime.timedelta(minutes=90)
    assert period.rms_drift_radius == pytest.approx(0.0)
    # Only the stay's first and last positions stay active, plus everything after it.
    assert minutes_of(split.active_records) == [0, 90, 100]


def test_split_on_movement_keeps_a_short_stay_as_active_records():
    track = stay_then_leave("1", 50)

    _, split = transform().split_on_movement(("1", track))

    assert split.stationary_periods == []
    assert minutes_of(split.active_records) == [0, 10, 20, 30, 40, 50, 60]


def test_split_on_movement_never_closes_a_stay_at_the_end_of_the_track():
    """Documents current behavior: a vessel still staying when the track ends has no stay."""
    track = stay("1", 0, 300)

    _, split = transform().split_on_movement(("1", track))

    assert split.stationary_periods == []
    assert len(split.active_records) == len(track)


def create(tracks, min_unique_vessels=1, fishing_vessels=()):
    """Runs CreateAnchorageLocations' steps on `tracks` without Beam.

    Groups the stationary periods and active records by S2 cell as its CoGroupByKey does.
    """
    target = transform(min_unique_vessels)
    cells = collections.defaultdict(lambda: ([], []))
    for track in tracks:
        split = target.split_on_movement(track)
        for token, value in target.extract_stationary(split):
            cells[token][0].append(value)
        for token, value in target.extract_active(split):
            cells[token][1].append(value)
    locations = [
        location
        for item in cells.items()
        for location in target.create_anchorage_pts(item, fishing_vessels)
    ]
    return [location for location in locations if target.has_enough_vessels(location)]


def test_creates_one_anchorage_location_per_cell_where_vessels_stay():
    tracks = [("1", stay_then_leave("1", 90)), ("2", stay_then_leave("2", 120))]

    (loc,) = create(tracks, min_unique_vessels=2)

    assert loc.s2id == cmn.LatLon(LAT, LON).S2CellId(cmn.ANCHORAGES_S2_SCALE).to_token()
    assert loc.mean_location.lat == pytest.approx(LAT)
    assert loc.mean_location.lon == pytest.approx(LON)
    assert loc.vessels == frozenset({"1", "2"})
    assert loc.total_visits == 2
    assert loc.stationary_ssvid_days == pytest.approx((90 + 120) / (24 * 60))
    assert loc.fishing_vessels == frozenset()


def test_counts_fishing_vessels_from_the_fishing_vessel_list():
    tracks = [("1", stay_then_leave("1", 90)), ("2", stay_then_leave("2", 90))]

    (loc,) = create(tracks, fishing_vessels=["1"])

    assert loc.fishing_vessels == frozenset({"1"})
    assert loc.stationary_fishing_ssvid_days == pytest.approx(90 / (24 * 60))


def test_drops_cells_with_fewer_than_min_unique_vessels():
    tracks = [("1", stay_then_leave("1", 90)), ("2", stay_then_leave("2", 90))]

    assert create(tracks, min_unique_vessels=3) == []


def test_vessels_only_passing_through_a_cell_do_not_count_towards_min_unique_vessels():
    """Documents current behavior: only vessels with a stay in the cell count as its vessels."""
    passing = stay("2", 0, 20) + [location("2", 30, lat=FAR_LAT, destination="")]
    tracks = [("1", stay_then_leave("1", 90)), ("2", passing)]

    (loc,) = create(tracks, min_unique_vessels=1)

    assert loc.vessels == frozenset({"1"})
    assert loc.total_ssvids == 2


def test_top_destination_is_the_most_common_destination_of_the_stays():
    tracks = [
        ("1", stay_then_leave("1", 90, destination="buenos aires")),
        ("2", stay_then_leave("2", 90, destination="BUENOS AIRES")),
        ("3", stay_then_leave("3", 90, destination="MONTEVIDEO")),
    ]

    (loc,) = create(tracks)

    assert loc.top_destination == "BUENOS AIRES"


def test_destinations_must_be_strings():
    """Documents the input contract: CreateTaggedRecords always sets destinations to strings
    ("" if unknown); a None destination fails when a cell is summarized."""
    _, split = transform().split_on_movement(("1", stay_then_leave("1", 90, destination=None)))
    cell = cmn.LatLon(LAT, LON).S2CellId(cmn.ANCHORAGES_S2_SCALE).to_token()
    item = (cell, ([("1", split.stationary_periods[0])], []))

    with pytest.raises(AttributeError):
        transform().create_anchorage_pts(item, [])
