import datetime

import pytz

from pipe_anchorages.common import LatLon
from pipe_anchorages.transforms.create_in_out_events import CreateInOutEvents
from pipe_anchorages.transforms.smart_thin_records import VisitLocationRecord


def _utc(year, month, day, hour=0, minute=0):
    return datetime.datetime(year, month, day, hour, minute, tzinfo=pytz.utc)


def _make_in_out_events(end_date):
    return CreateInOutEvents(
        anchorage_entry_dist=3.0,
        anchorage_exit_dist=4.0,
        stopped_begin_speed=0.2,
        stopped_end_speed=0.5,
        min_gap_minutes=240.0,
        end_date=end_date,
    )


class TestGapBeginAnchorageOnSilentTransit:
    """When a vessel goes silent at one anchorage and reappears at another,
    PORT_GAP_BEGIN must be located at the *pre-gap* anchorage.

    `start_timestamp` for a visit that begins with PORT_GAP_BEGIN is
    `T_lastInPort + min_gap` -- a moment at which the only thing the data
    tells us about the vessel's location is the last in-port observation.
    Tagging that synthetic timestamp with the post-gap anchorage describes
    the visit as being somewhere the vessel had not yet reached.
    """

    identifier = ("ssvid", "vessel_id", "seg_id")
    # t1 sits close enough to end-of-day that the trailing-gap branch in
    # `_create_in_out_events` does not fire (last_possible_timestamp - t1 < min_gap),
    # leaving us with only the inline gap pair to assert against.
    t0 = _utc(2024, 1, 2, 15, 0)
    t1 = _utc(2024, 1, 2, 21, 0)  # 6h after t0, > min_gap
    end_date = _utc(2024, 1, 2)

    rcd_at_a = VisitLocationRecord(
        identifier=identifier,
        timestamp=t0,
        location=LatLon(45.0, -100.0),
        speed=0.0,
        is_possible_gap_end=False,
        port_s2id="anchA",
        port_dist=1.0,
        port_lat=45.0,
        port_lon=-100.0,
    )
    rcd_at_b = VisitLocationRecord(
        identifier=identifier,
        timestamp=t1,
        location=LatLon(46.0, -101.0),
        speed=0.0,
        is_possible_gap_end=True,
        port_s2id="anchB",
        port_dist=1.0,
        port_lat=46.0,
        port_lon=-101.0,
    )

    def _events(self):
        in_out = _make_in_out_events(end_date=self.end_date)
        _, events = in_out.create_in_out_events(
            (self.identifier, [self.rcd_at_a, self.rcd_at_b])
        )
        return events

    def test_gap_pair_is_emitted_with_no_other_events(self):
        events = self._events()
        types = [e.event_type for e in events]
        assert types == ["PORT_GAP_END", "PORT_GAP_BEGIN"]

    def test_gap_end_anchored_to_resuming_record(self):
        gap_end = next(e for e in self._events() if e.event_type == "PORT_GAP_END")
        assert gap_end.timestamp == self.t1
        assert gap_end.anchorage_id == "anchB"
        assert gap_end.lat == 46.0
        assert gap_end.lon == -101.0

    def test_gap_begin_timestamp_is_last_in_port_plus_min_gap(self):
        gap_begin = next(
            e for e in self._events() if e.event_type == "PORT_GAP_BEGIN"
        )
        assert gap_begin.timestamp == self.t0 + datetime.timedelta(minutes=240)

    def test_gap_begin_anchorage_matches_pre_gap_position(self):
        gap_begin = next(e for e in self._events() if e.event_type == "PORT_GAP_BEGIN")
        assert gap_begin.anchorage_id == "anchA"
        assert gap_begin.lat == 45.0
        assert gap_begin.lon == -100.0
