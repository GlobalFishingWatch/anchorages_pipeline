from collections import Counter, namedtuple
from datetime import datetime, timedelta
from itertools import groupby
from typing import Iterable, Iterator

import apache_beam as beam
from pipe_anchorages import common as cmn
from pipe_anchorages.core.visit_event import VisitEvent
from pipe_anchorages.transforms.in_out_events import InOutEventsBase
from pipe_anchorages.core.visit_location_record import VisitLocationRecord

PseudoRcd = namedtuple("PseudoRcd", ["location", "timestamp", "ssvid", "vessel_id", "seg_id"])


class CreateInOutEvents(beam.PTransform, InOutEventsBase):
    """Turns each vessel's port state transitions into port events.

    Input: (vessel_id, records) pairs of VisitLocationRecord.
    Output: (vessel_id, events) pairs of VisitEvent.

    Walks each vessel's records in time order, keeping one record per timestamp (see
    `_one_record_per_instant`), through InOutEventsBase's state machine (AT_SEA, IN_PORT,
    STOPPED), emitting PORT_ENTRY, PORT_EXIT, PORT_STOP_BEGIN and PORT_STOP_END on state
    changes. In port, a gap of at least `min_gap_minutes` before a record that may end a gap
    emits PORT_GAP_END at that record and PORT_GAP_BEGIN at `min_gap_minutes` after the
    previous one. A vessel still in port at the end of the range, with no record for at least
    `min_gap_minutes` before the range's last possible timestamp (just before the exclusive
    `end_time`), gets a PORT_GAP_BEGIN.

    Every event is located at the last anchorage the vessel was in port at.
    """

    def __init__(
        self,
        anchorage_entry_dist: float,
        anchorage_exit_dist: float,
        stopped_begin_speed: float,
        stopped_end_speed: float,
        min_gap_minutes: float,
        end_time: datetime,
    ) -> None:
        super().__init__()
        self.anchorage_entry_dist = anchorage_entry_dist
        self.anchorage_exit_dist = anchorage_exit_dist
        self.stopped_begin_speed = stopped_begin_speed
        self.stopped_end_speed = stopped_end_speed
        self.min_gap = timedelta(minutes=min_gap_minutes)
        self.end_time = end_time
        self.last_possible_timestamp = end_time - timedelta(microseconds=1)
        assert self.min_gap < timedelta(
            days=1
        ), "min gap must be under one day in current implementation"

    def _build_event(self, active_port_rcd, rcd, event_type: str, last_timestamp) -> VisitEvent:
        """Builds an event of `rcd`'s vessel, at the anchorage of `active_port_rcd`."""
        return VisitEvent(
            anchorage_id=active_port_rcd.port_s2id,
            lat=active_port_rcd.port_lat,
            lon=active_port_rcd.port_lon,
            vessel_lat=rcd.location.lat,
            vessel_lon=rcd.location.lon,
            ssvid=rcd.ssvid,
            seg_id=rcd.seg_id,
            vessel_id=rcd.vessel_id,
            timestamp=rcd.timestamp,
            event_type=event_type,
            last_timestamp=last_timestamp,
        )

    def _yield_gap_beg(
        self, gap_end_rcd, last_timestamp, active_port_rcd
    ) -> Iterator[VisitEvent]:
        """Yields the PORT_GAP_BEGIN, min_gap after `last_timestamp`, of a gap ending at
        `gap_end_rcd`."""
        evt_timestamp = last_timestamp + self.min_gap
        assert evt_timestamp <= gap_end_rcd.timestamp
        rcd = PseudoRcd(
            location=cmn.LatLon(None, None),
            timestamp=evt_timestamp,
            ssvid=gap_end_rcd.ssvid,
            vessel_id=gap_end_rcd.vessel_id,
            seg_id=gap_end_rcd.seg_id,
        )
        yield self._build_event(active_port_rcd, rcd, self.EVT_GAP_BEG, last_timestamp)

    @staticmethod
    def _one_record_per_instant(
        records: Iterable[VisitLocationRecord],
    ) -> list[VisitLocationRecord]:
        """Returns `records` in time order, keeping one record per timestamp.

        A vessel can have records of several segments at the same instant, e.g. when a feed
        delivers many messages stamped with the same second and the segmenter opens a
        single-point segment for each of them. Walking all of them through the state machine
        flips the vessel in and out of port at that instant, which yields zero-duration visits
        and duplicate visit_ids. Of the records at one instant, this keeps the one of the
        segment with the most records in the vessel's history, so an established track wins
        over single points; ties break by seg_id (as strings, so "-10" sorts before "-2"),
        which keeps the result independent of the order Beam delivers the records in.

        A segment has at most one record per timestamp, so records without a tie are kept
        as they are, including lone single-point segments.
        """
        records = list(records)
        records_per_segment = Counter(x.seg_id for x in records)
        records = sorted(
            records, key=lambda x: (x.timestamp, -records_per_segment[x.seg_id], x.seg_id)
        )
        return [next(tied) for _, tied in groupby(records, key=lambda x: x.timestamp)]

    def _create_in_out_events(
        self, records: Iterable[VisitLocationRecord]
    ) -> Iterator[VisitEvent]:
        """Yields the events of one vessel's records (see the class docstring)."""
        records = self._one_record_per_instant(records)
        rcd = None
        last_state = None
        active_port_rcd = None
        last_timestamp = None
        for rcd in records:
            is_in_port = self._is_in_port(last_state, rcd.port_dist)
            active_port_rcd = rcd if is_in_port else active_port_rcd
            is_stopped = self._is_stopped(last_state, rcd.speed)
            state = self._compute_state(is_in_port, is_stopped)

            if last_timestamp is not None:
                if (
                    last_state in self.in_port_states
                    and rcd.is_possible_gap_end
                    and rcd.timestamp - last_timestamp >= self.min_gap
                ):
                    yield self._build_event(active_port_rcd, rcd, self.EVT_GAP_END, last_timestamp)
                    yield from self._yield_gap_beg(rcd, last_timestamp, active_port_rcd)

            for event_type in self.transition_map[(last_state, state)]:
                yield self._build_event(active_port_rcd, rcd, event_type, last_timestamp)

            last_timestamp = rcd.timestamp
            last_state = state
        if (
            # Trigger gap ends for gaps that started but we haven't reached their end
            last_state in self.in_port_states
            and self.last_possible_timestamp - last_timestamp >= self.min_gap
        ):
            psuedo_rcd = rcd._replace(timestamp=self.last_possible_timestamp)
            yield from self._yield_gap_beg(psuedo_rcd, last_timestamp, active_port_rcd)

    # No type hints: Beam would infer coders from them (see core.py).
    def create_in_out_events(self, grouped_records):
        """Returns (vessel_id, events) for a (vessel_id, records) pair."""
        identity, records = grouped_records
        return identity, list(self._create_in_out_events(records))

    def expand(self, grouped_records: beam.PCollection) -> beam.PCollection:
        return grouped_records | beam.Map(self.create_in_out_events)
