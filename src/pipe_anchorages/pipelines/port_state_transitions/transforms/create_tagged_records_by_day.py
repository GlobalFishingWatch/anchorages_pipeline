import apache_beam as beam

from pipe_anchorages.records import VesselInfoRecord, VesselLocationRecord


class CreateTaggedRecordsByDay(beam.PTransform):
    """Groups each vessel's records by day, deduplicated and tagged with their destination.

    Input: (identifier, record) pairs of VesselInfoRecord and VesselLocationRecord.
    Output: ((identifier, date), records) pairs of VesselLocationRecord, sorted by timestamp,
    keeping one record per timestamp. Each location record gets the destination of the last
    info record before it that day (empty if there was none).
    """

    # No type hints on the methods Beam maps: it would infer coders from them.
    def add_date_to_key(self, item):
        identity, value = item
        return (identity, str(value.timestamp.date())), value

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
            | beam.Map(self.add_date_to_key)
            | beam.GroupByKey()
            | beam.Map(self.order_by_timestamp)
            | beam.Map(self.dedup_by_timestamp)
            | beam.Map(self.tag_records)
        )
