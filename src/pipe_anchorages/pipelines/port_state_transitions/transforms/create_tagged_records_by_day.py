import apache_beam as beam

from pipe_anchorages.transforms.shrink_group_by import ShrinkGroupBy


class CreateTaggedRecordsByDay(beam.PTransform):
    """Groups each vessel's records by day, deduplicated and with an empty destination.

    Input: records of `record_type`, one record type per input.
    Output: ((identifier, date), records) pairs, sorted by timestamp, keeping one record per
    timestamp. The records go through the GroupByKey shrunken by ShrinkGroupBy.
    """

    def __init__(self, record_type: type, keys=("identifier",)) -> None:
        super().__init__()
        self.record_type = record_type
        self.keys = keys

    # No type hints on the methods Beam maps: it would infer coders from them.
    def record_date(self, record):
        return str(record.timestamp.date())

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
            | ShrinkGroupBy(
                *self.keys,
                record_type=self.record_type,
                elements="Positions",
                date=self.record_date,
            )
            | beam.Map(self.order_by_timestamp)
            | beam.Map(self.dedup_by_timestamp)
            | beam.Map(self.tag_records)
        )
