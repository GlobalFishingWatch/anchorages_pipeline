import apache_beam as beam

from pipe_anchorages import common as cmn
from pipe_anchorages.core.namedtuples import datetime_to_s
from pipe_anchorages.transforms.create_tagged_anchorages import CreateTaggedAnchorages
from pipe_anchorages.transforms.smart_thin_records import SmartThinRecords


# No type hints on the functions Beam maps: it would infer coders from them.
def record_to_msg(x):
    """Converts a VisitLocationRecord into a row of the output table."""
    x = x._asdict()
    if x["timestamp"] is not None:
        x["timestamp"] = datetime_to_s(x["timestamp"])
    location = x.pop("location")
    x["lon"] = location.lon
    x["lat"] = location.lat
    return x


class FindPortStateTransitions(beam.PTransform):
    """Turns position messages (query rows) into port state transitions table records (row dicts).

    Groups the inline chain (CreateVesselRecords -> CreateTaggedRecordsByDay -> SmartThinRecords
    -> record_to_msg) into one composite PTransform, since LinearDag's core slot takes exactly one
    transform. The named anchorages are NOT read here: they arrive as a side input that LinearDag
    applies from `side_inputs=` and delivers via `set_side_inputs`, which this class implements.

    Constructor params are the raw config fields, not a config object.
    """

    def __init__(
        self,
        anchorage_entry_dist_km,
        anchorage_exit_dist_km,
        stopping_speed_knots,
        starting_speed_knots,
        min_anchorage_gap_minutes,
        start_date,
        end_date,
    ) -> None:
        super().__init__()
        self.anchorage_entry_dist_km = anchorage_entry_dist_km
        self.anchorage_exit_dist_km = anchorage_exit_dist_km
        self.stopping_speed_knots = stopping_speed_knots
        self.starting_speed_knots = starting_speed_knots
        self.min_anchorage_gap_minutes = min_anchorage_gap_minutes
        self.start_date = start_date
        self.end_date = end_date
        self._named_anchorages = None

    def set_side_inputs(self, side_inputs):
        self._named_anchorages = side_inputs

    def expand(self, xs):
        anchorages = self._named_anchorages | CreateTaggedAnchorages()

        return (
            xs
            | cmn.CreateVesselRecords(destination=None)
            | cmn.CreateTaggedRecordsByDay()
            | "ThinRecords" >> SmartThinRecords(
                anchorages=anchorages,
                anchorage_entry_dist=self.anchorage_entry_dist_km,
                anchorage_exit_dist=self.anchorage_exit_dist_km,
                stopped_begin_speed=self.stopping_speed_knots,
                stopped_end_speed=self.starting_speed_knots,
                min_gap_minutes=self.min_anchorage_gap_minutes,
                start_date=self.start_date,
                end_date=self.end_date,
            )
            | "RecordToMsg" >> beam.Map(record_to_msg)
        )
