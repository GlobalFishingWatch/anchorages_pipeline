import hashlib
import logging
import math
from typing import Iterator

import apache_beam as beam
import six
from pipe_anchorages.core.port_visit import PortVisit
from pipe_anchorages.core.visit_event import VisitEvent


class CreatePortVisits(beam.PTransform):
    """Groups each vessel's port events into port visits.

    Input: (vessel_id, events) pairs, as CreateInOutEvents outputs them.
    Output: one PortVisit per visit.

    Events are sorted by timestamp (and, at the same timestamp, by EVENT_TYPES order).
    A visit starts at a PORT_ENTRY, or when consecutive events are on segments more than
    `max_interseg_dist_nm` apart, and ends at a PORT_EXIT or the vessel's last event.
    Events of an unknown type are logged and dropped.
    """

    EVENT_TYPES = [
        "PORT_ENTRY",
        # The order of PORT_GAP_XXX is somewhat arbitrary, but it
        # Shouldn't matter as long as it occurs between ENTRY
        # and EXIT.
        "PORT_GAP_BEGIN",
        "PORT_GAP_END",
        "PORT_STOP_BEGIN",
        "PORT_STOP_END",
        "PORT_EXIT",
    ]

    TYPE_ORDER = {x: i for (i, x) in enumerate(EVENT_TYPES)}

    MAX_EMITTED_EVENTS = 200

    def __init__(self, max_interseg_dist_nm: float) -> None:
        super().__init__()
        self.max_interseg_dist_nm = max_interseg_dist_nm

    def compute_confidence(self, events: list[VisitEvent]) -> int:
        """Returns how confident we are that `events` are a real visit, from 1 to 4.

        4: a stop or gap, plus both an entry and an exit.
        3: a stop or gap, plus an entry or an exit.
        2: only a stop or gap.
        1: only an entry and/or an exit.
        """
        event_types = set(x.event_type for x in events)
        has_stop = ("PORT_STOP_BEGIN" in event_types) or ("PORT_STOP_END" in event_types)
        has_gap = ("PORT_GAP_BEGIN" in event_types) or ("PORT_GAP_END" in event_types)
        has_entry = "PORT_ENTRY" in event_types
        has_exit = "PORT_EXIT" in event_types
        if (has_stop or has_gap) and (has_entry and has_exit):
            return 4
        if (has_stop or has_gap) and (has_entry or has_exit):
            return 3
        if has_stop or has_gap:
            return 2
        if has_entry or has_exit:
            return 1
        raise ValueError(f"`events` missing expected event types. Has {set(event_types)}")

    def prune_events(self, events: list[VisitEvent]) -> list[VisitEvent]:
        """Keeps the first and last MAX_EMITTED_EVENTS / 2 events of a visit with more."""
        if len(events) > self.MAX_EMITTED_EVENTS:
            n = self.MAX_EMITTED_EVENTS // 2
            events = events[:n] + events[-n:]
        return events

    def create_visit(self, id_: tuple, visit_events: list[VisitEvent]) -> PortVisit:
        """Builds the PortVisit of `visit_events`, for the vessel `id_` = (ssvid, vessel_id).

        The visit starts at its first event and ends at its last. visit_id is the MD5 of the
        vessel_id and the first event's timestamp and anchorage location.
        """
        ssvid, vessel_id = id_
        raw_visit_id = "{}-{}-{}-{}".format(
            vessel_id,
            visit_events[0].timestamp.isoformat(),
            visit_events[0].lon,
            visit_events[0].lat,
        )
        duration_hrs = (visit_events[-1].timestamp - visit_events[0].timestamp).total_seconds() / (
            60 * 60
        )
        return PortVisit(
            visit_id=hashlib.md5(six.ensure_binary(raw_visit_id)).hexdigest(),
            ssvid=str(ssvid),
            vessel_id=str(vessel_id),
            start_timestamp=visit_events[0].timestamp,
            start_lat=visit_events[0].lat,
            start_lon=visit_events[0].lon,
            start_anchorage_id=visit_events[0].anchorage_id,
            end_timestamp=visit_events[-1].timestamp,
            end_lat=visit_events[-1].lat,
            end_lon=visit_events[-1].lon,
            end_anchorage_id=visit_events[-1].anchorage_id,
            duration_hrs=duration_hrs,
            confidence=self.compute_confidence(visit_events),
            events=self.prune_events(visit_events),
        )

    def possibly_yield_visit(self, id_: tuple, events: list[VisitEvent]) -> Iterator[PortVisit]:
        """Yields the visit of `events`, if there are any."""
        if events:
            yield self.create_visit(id_, events)

    def has_large_interseg_dist(self, evt1: VisitEvent, evt2: VisitEvent) -> bool:
        """Returns whether two events on different segments are over max_interseg_dist_nm apart.

        Uses an equirectangular approximation of the distance between the events' anchorages.
        """
        if evt1.seg_id == evt2.seg_id:
            return False
        dlat = evt2.lat - evt1.lat
        lat = 0.5 * (evt1.lat + evt2.lat)
        scale = math.cos(math.radians(lat))
        dlon = evt2.lon - evt1.lon
        # Ensure dlon is in range [-180, 180]
        # so that we don't have trouble near the dateline
        dlon = (dlon + 180) % 360 - 180
        dist_nm = math.hypot(dlat, scale * dlon) * 60
        return dist_nm > self.max_interseg_dist_nm

    # No type hints: Beam would infer coders from them (PortVisitCoder can't encode events).
    def create_port_visits(self, tagged_events):
        """Yields the visits of one vessel's (grouping id, events) pair.

        N.B. `evt.event_type in "PORT_ENTRY"` is a substring check, not an equality one;
        it behaves the same for every type in EVENT_TYPES.
        """
        grouping_id, events = tagged_events
        if not len(events):
            return
        id_ = events[0].ssvid, events[0].vessel_id
        # Sort events by timestamp, and also so that enter, stop, start,
        # exit are in the correct order.
        tagged = [(x.timestamp, self.TYPE_ORDER[x.event_type], x) for x in events]
        tagged.sort()
        ordered_events = [x for (_, _, x) in tagged]

        visit_events = []
        for i, evt in enumerate(ordered_events):
            has_large_gap = (
                self.has_large_interseg_dist(visit_events[-1], evt) if visit_events else False
            )
            if evt.event_type in "PORT_ENTRY" or has_large_gap:
                yield from self.possibly_yield_visit(id_, visit_events)
                visit_events = []
            if evt.event_type not in self.EVENT_TYPES:
                logging.error(f'Unknown event type "{evt.event_type}", discarding.')
                continue
            visit_events.append(evt)
            is_last = i == len(ordered_events) - 1
            if (evt.event_type == "PORT_EXIT") or is_last:
                yield from self.possibly_yield_visit(id_, visit_events)
                visit_events = []

    def expand(self, grouped_records: beam.PCollection) -> beam.PCollection:
        return grouped_records | beam.FlatMap(self.create_port_visits)
