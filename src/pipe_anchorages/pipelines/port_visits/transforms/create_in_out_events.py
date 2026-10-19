from collections import namedtuple
from datetime import datetime, timedelta
from typing import Iterable, Iterator

import apache_beam as beam
from pipe_anchorages import common as cmn
from pipe_anchorages.core.visit_event import VisitEvent
from pipe_anchorages.transforms.in_out_events import InOutEventsBase
from pipe_anchorages.transforms.smart_thin_records import VisitLocationRecord

PseudoRcd = namedtuple("PseudoRcd", ["location", "timestamp", "identifier"])


class CreateInOutEvents(beam.PTransform, InOutEventsBase):
    """Turns each vessel's port state transitions into port events.

    Input: (vessel_id, records) pairs of VisitLocationRecord.
    Output: (vessel_id, events) pairs of VisitEvent.

    Walks each vessel's records in time order through InOutEventsBase's state machine
    (AT_SEA, IN_PORT, STOPPED), emitting PORT_ENTRY, PORT_EXIT, PORT_STOP_BEGIN and
    PORT_STOP_END on state changes. In port, a gap of at least `min_gap_minutes` before a
    record that may end a gap emits PORT_GAP_END at that record and PORT_GAP_BEGIN at
    `min_gap_minutes` after the previous one. A vessel still in port at the end of the
    range, with no record for at least `min_gap_minutes` before the range's last
    possible timestamp (just before the exclusive `end_time`), gets a PORT_GAP_BEGIN.

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
        ssvid, vessel_id, seg_id = rcd.identifier
        return VisitEvent(
            anchorage_id=active_port_rcd.port_s2id,
            lat=active_port_rcd.port_lat,
            lon=active_port_rcd.port_lon,
            vessel_lat=rcd.location.lat,
            vessel_lon=rcd.location.lon,
            ssvid=ssvid,
            seg_id=seg_id,
            vessel_id=vessel_id,
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
            identifier=gap_end_rcd.identifier,
        )
        yield self._build_event(active_port_rcd, rcd, self.EVT_GAP_BEG, last_timestamp)

    def _create_in_out_events(
        self, records: Iterable[VisitLocationRecord]
    ) -> Iterator[VisitEvent]:
        """Yields the events of one vessel's records (see the class docstring)."""
        records = sorted(records, key=lambda x: x.timestamp)
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
