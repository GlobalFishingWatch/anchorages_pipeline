import datetime
import math
from collections import namedtuple
from collections.abc import Iterable

import apache_beam as beam

from pipe_anchorages import common as cmn
from pipe_anchorages.distance import distance
from pipe_anchorages.core.anchorage_location import AnchorageLocation
from pipe_anchorages.records import VesselLocationRecord

StationaryPeriod = namedtuple(
    "StationaryPeriod", ["location", "start_time", "duration", "rms_drift_radius", "destination"]
)
"""A vessel's stay at one place: mean location, start, duration, RMS drift radius, destination."""

ActiveAndStationary = namedtuple("ActiveAndStationary", ["active_records", "stationary_periods"])
"""A vessel's track split into active location records and stationary periods."""

VesselTrack = tuple[str, list[VesselLocationRecord]]
"""A vessel identifier and its location records, in time order."""

VesselSplit = tuple[str, ActiveAndStationary]
"""A vessel identifier and its track, split into active records and stationary periods."""

CellVisits = tuple[
    str, tuple[Iterable[tuple[str, StationaryPeriod]], Iterable[tuple[str, VesselLocationRecord]]]
]
"""An S2 cell token and the ``(vessel id, stationary period)`` and ``(vessel id, active record)``
pairs that fall in it."""


class CreateAnchorageLocations(beam.PTransform):
    """Turns vessel tracks into one :class:`AnchorageLocation` per S2 cell where vessels stay.

    Input: one :data:`VesselTrack` per vessel (see ``CreateTaggedRecords``). Output: one
    :class:`AnchorageLocation` per anchorage-scale S2 cell (``ANCHORAGES_S2_SCALE``, cells of about
    0.5 km) with enough vessels staying in it.

    1. Each vessel's track is split into stationary periods and active records
       (see :meth:`split_on_movement`).
    2. Stationary periods are keyed by the S2 cell of their mean location, and active records by
       the S2 cell of their location; both are grouped per cell.
    3. Each cell with at least one stationary period is summarized into an
       :class:`AnchorageLocation` (visits, vessels, fishing vessels, stationary and active
       vessel-days, drift radius, top destination); cells with only active records are dropped.
    4. Cells where fewer than ``min_unique_vessels`` distinct vessels stayed are dropped (vessels
       that were only active in the cell don't count).
    """

    def __init__(
        self,
        min_duration: datetime.timedelta,
        max_distance: float,
        min_unique_vessels: int,
        fishing_vessel_list: Iterable[str],
    ) -> None:
        """
        Args:
            min_duration:
                A stay must last longer than this to be a stationary period.

            max_distance:
                Distance in km from the stay's first position beyond which the vessel has left.

            min_unique_vessels:
                Minimum number of distinct vessels staying in a cell for it to be kept.

            fishing_vessel_list:
                Identifiers of fishing vessels (usually a side input), used to count fishing
                vessels and fishing vessel-days per cell.
        """
        super().__init__()
        self.min_duration = min_duration
        self.max_distance = max_distance
        self.min_unique_vessels = min_unique_vessels
        self.fishing_vessel_list = fishing_vessel_list
        self.fishing_vessel_set = None

    def split_on_movement(self, item: VesselTrack) -> VesselSplit:
        """Splits a vessel's track into stationary periods and active records.

        A stay starts at a position and lasts while the following positions are within
        ``max_distance`` of that first position. When a position falls beyond it, the stay ends:

        - if it lasted longer than ``min_duration``, it becomes a :class:`StationaryPeriod`
          (mean location, start, duration, RMS drift radius, first position's destination), and
          only its first and last positions are kept as active records;
        - otherwise all its positions are active records.

        The position beyond ``max_distance`` starts the next stay. The last stay never becomes a
        stationary period: a vessel still staying at the end of the track has all those
        positions counted as active records.
        """
        ssvid, records = item

        active_records = []
        stationary_periods = []
        current_period = []

        for rcd in records:
            if current_period:
                first_rcd = current_period[0]
                if distance(rcd.location, first_rcd.location) > self.max_distance:
                    if current_period[-1].timestamp - first_rcd.timestamp > self.min_duration:
                        active_records.append(first_rcd)
                        if current_period[-1] != first_rcd:
                            active_records.append(current_period[-1])
                        num_points = len(current_period)
                        duration = current_period[-1].timestamp - first_rcd.timestamp
                        mean_lat = sum(x.location.lat for x in current_period) / num_points
                        mean_lon = sum(x.location.lon for x in current_period) / num_points
                        mean_location = cmn.LatLon(mean_lat, mean_lon)
                        rms_drift_radius = math.sqrt(
                            sum(distance(x.location, mean_location) ** 2 for x in current_period)
                            / num_points
                        )
                        stationary_periods.append(
                            StationaryPeriod(
                                mean_location,
                                first_rcd.timestamp,
                                duration,
                                rms_drift_radius,
                                first_rcd.destination,
                            )
                        )
                    else:
                        active_records.extend(current_period)
                    current_period = []
            current_period.append(rcd)
        active_records.extend(current_period)

        return (
            ssvid,
            ActiveAndStationary(
                active_records=active_records, stationary_periods=stationary_periods
            ),
        )

    def extract_stationary(
        self, item: VesselSplit
    ) -> list[tuple[str, tuple[str, StationaryPeriod]]]:
        """Keys each stationary period by the S2 cell token of its mean location."""
        ssvid, combined = item
        return [
            (sp.location.S2CellId(cmn.ANCHORAGES_S2_SCALE).to_token(), (ssvid, sp))
            for sp in combined.stationary_periods
        ]

    def extract_active(
        self, item: VesselSplit
    ) -> list[tuple[str, tuple[str, VesselLocationRecord]]]:
        """Keys each active record by the S2 cell token of its location."""
        ssvid, combined = item
        return [
            (ar.location.S2CellId(cmn.ANCHORAGES_S2_SCALE).to_token(), (ssvid, ar))
            for ar in combined.active_records
        ]

    def create_anchorage_pts(
        self, item: CellVisits, fishing_vessel_list: Iterable[str]
    ) -> list[AnchorageLocation]:
        """Summarizes a cell's visits into an :class:`AnchorageLocation`, if it has any stay."""
        if self.fishing_vessel_set is None:
            self.fishing_vessel_set = set(fishing_vessel_list)
        value = AnchorageLocation.from_cell_visits(item, self.fishing_vessel_set)
        if value is None:
            return []

        return [value]

    def has_enough_vessels(self, item: AnchorageLocation) -> bool:
        """Whether at least ``min_unique_vessels`` distinct vessels stayed in the cell."""
        return len(item.vessels) >= self.min_unique_vessels

    def expand(self, ais_source: beam.PCollection) -> beam.PCollection:
        combined = ais_source | beam.Map(self.split_on_movement)
        stationary = combined | beam.FlatMap(self.extract_stationary)
        active = combined | beam.FlatMap(self.extract_active)
        return (
            (stationary, active)
            | beam.CoGroupByKey()
            | beam.FlatMap(self.create_anchorage_pts, self.fishing_vessel_list)
            | beam.Filter(self.has_enough_vessels)
        )
