import apache_beam as beam

from pipe_anchorages.records import VesselRecord


class CreateTaggedRecordsByDay(beam.PTransform):
    """Groups each vessel's records by day, deduplicated and with an empty destination.

    Input: (identifier, record) pairs of `record_type`, one record type per input.
    Output: ((identifier, date), records) pairs, sorted by timestamp, keeping one record per
    timestamp. The records go through the GroupByKey in the form of record_type's to_shuffle
    and from_shuffle.
    """

    def __init__(self, record_type: type[VesselRecord]) -> None:
        super().__init__()
        self.record_type = record_type

    # No type hints on the methods Beam maps: it would infer coders from them.
    def add_date_to_key(self, item):
        identity, value = item
        return (identity, str(value.timestamp.date())), value

    def to_shuffle(self, item):
        """Converts the record to the form it goes through the GroupByKey in."""
        key, value = item
        return key, value.to_shuffle()

    def from_shuffle(self, item):
        """Rebuilds the records to_shuffle converted; the key's identifier is theirs."""
        key, values = item
        identity, _ = key
        return key, [self.record_type.from_shuffle(identity, v) for v in values]

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
        """Gives each record an empty destination.

        Port-state-transitions has no info records to take a destination from: its rows have
        destination=None, so CreateVesselRecords drops the ones without a position as invalid.
        """
        ident, records = item
        return (ident, [rcd._replace(destination="") for rcd in records])

    def expand(self, vessel_records: beam.PCollection) -> beam.PCollection:
        return (
            vessel_records
            | beam.Map(self.add_date_to_key)
            | "ToShuffle" >> beam.Map(self.to_shuffle)
            | beam.GroupByKey()
            | "FromShuffle" >> beam.Map(self.from_shuffle)
            | beam.Map(self.order_by_timestamp)
            | beam.Map(self.dedup_by_timestamp)
            | beam.Map(self.tag_records)
        )
