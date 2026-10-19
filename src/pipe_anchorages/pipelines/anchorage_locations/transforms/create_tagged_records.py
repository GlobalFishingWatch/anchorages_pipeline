import datetime
from collections.abc import Iterable

import apache_beam as beam

from pipe_anchorages.records import VesselInfoRecord, VesselLocationRecord

VesselRecords = tuple[str, Iterable[VesselInfoRecord | VesselLocationRecord]]
"""A vessel identifier and its records."""

VesselTrack = tuple[str, list[VesselLocationRecord]]
"""A vessel identifier and its location records, in time order."""


class CreateTaggedRecords(beam.PTransform):
    """Turns keyed vessel records into one time-ordered, thinned track per vessel.

    Input: ``(vessel id, record)`` pairs, with records being :class:`VesselLocationRecord` or
    :class:`VesselInfoRecord`. Output: one :data:`VesselTrack` per vessel with enough positions.

    For each vessel, records are:

    1. grouped and sorted by timestamp (then speed and location, for determinism);
    2. de-duplicated: only the first record at each timestamp is kept;
    3. dropped altogether if fewer than ``min_required_positions`` remain;
    4. tagged: each location record gets the destination of the latest info record before it
       (``""`` if none), and info records are dropped;
    5. thinned, if ``thin``: a position is kept only if at least 5 minutes after the previous
       kept one.

    Note that ``min_required_positions`` is checked before thinning, so a vessel can have
    fewer positions than that in its final track.
    """

    FIVE_MINUTES = datetime.timedelta(minutes=5)

    def __init__(self, min_required_positions: int, thin: bool = True) -> None:
        """
        Args:
            min_required_positions:
                Minimum number of (de-duplicated) records a vessel needs to be kept.

            thin:
                Whether to thin each track to positions at least 5 minutes apart.
        """
        super().__init__()
        self.min_required_positions = min_required_positions
        self.thin = thin

    def order_by_timestamp(self, item: VesselRecords) -> VesselRecords:
        """Sorts a vessel's records by timestamp, then speed and location."""
        ident, records = item
        records = sorted(records, key=lambda x: (x.timestamp, x.speed, x.location))
        return ident, records

    def dedup_by_timestamp(self, item: VesselRecords) -> VesselRecords:
        """Keeps only the first record (in sort order) at each timestamp."""
        key, source = item
        seen = set()
        sink = []
        for x in sorted(source, key=lambda x: (x.timestamp, x.speed, x.location)):
            if x.timestamp not in seen:
                sink.append(x)
                seen.add(x.timestamp)
        return (key, sink)

    def long_enough(self, item: VesselRecords) -> bool:
        """Whether a vessel has at least ``min_required_positions`` records."""
        ident, records = item
        return len(records) >= self.min_required_positions

    def thin_records(self, item: VesselTrack) -> VesselTrack:
        """Keeps only positions at least 5 minutes after the previous kept one."""
        if not self.thin:
            return item
        ident, records = item
        last_timestamp = datetime.datetime(datetime.MINYEAR, 1, 1, tzinfo=datetime.timezone.utc)
        thinned = []
        for rcd in records:
            if (rcd.timestamp - last_timestamp) >= self.FIVE_MINUTES:
                last_timestamp = rcd.timestamp
                thinned.append(rcd)
        return ident, thinned

    def tag_records(self, item: VesselRecords) -> VesselTrack:
        """Sets each location record's destination from the latest info record before it.

        Info records are dropped from the output.
        """
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

    def expand(self, vessel_records: beam.PCollection) -> beam.PCollection:
        return (
            vessel_records
            | beam.GroupByKey()
            | beam.Map(self.order_by_timestamp)
            | beam.Map(self.dedup_by_timestamp)
            | beam.Filter(self.long_enough)
            | beam.Map(self.tag_records)
            | beam.Map(self.thin_records)
        )
