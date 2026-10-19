from __future__ import absolute_import, print_function, division

import s2sphere
import math
from collections import namedtuple, Counter

from pipe_anchorages import common as cmn
from pipe_anchorages.port_name_filter import normalized_valid_names


class AnchorageLocation(
    namedtuple(
        "AnchorageLocation",
        [
            "mean_location",
            "total_visits",
            "vessels",
            "fishing_vessels",
            "rms_drift_radius",
            "top_destination",
            "s2id",
            "neighbor_s2ids",
            "active_ssvids",
            "total_ssvids",
            "stationary_ssvid_days",
            "stationary_fishing_ssvid_days",
            "active_ssvid_days",
        ],
    )
):
    __slots__ = ()

    @staticmethod
    def from_cell_visits(value, fishing_vessel_set):
        s2id, (stationary_periods, active_points) = value

        n = 0
        total_lat = 0.0
        total_lon = 0.0
        fishing_vessels = set()
        vessels = set()
        total_squared_drift_radius = 0.0
        active_ssvids = set(md for (md, loc) in active_points)
        active_ssvid_count = len(active_ssvids)
        active_days = len(set([(md, loc.timestamp.date()) for (md, loc) in active_points]))
        stationary_days = 0
        stationary_fishing_days = 0

        for ssvid, sp in stationary_periods:
            n += 1
            total_lat += sp.location.lat
            total_lon += sp.location.lon
            vessels.add(ssvid)
            stationary_days += sp.duration.total_seconds() / (24.0 * 60.0 * 60.0)
            if ssvid in fishing_vessel_set:
                fishing_vessels.add(ssvid)
                stationary_fishing_days += sp.duration.total_seconds() / (24.0 * 60.0 * 60.0)
            total_squared_drift_radius += sp.rms_drift_radius**2
        all_destinations = normalized_valid_names(
            sp.destination for (md, sp) in stationary_periods
        )

        total_ssvid_count = len(vessels | active_ssvids)

        if n:
            neighbor_s2ids = tuple(
                s2sphere.CellId.from_token(s2id).get_all_neighbors(cmn.ANCHORAGES_S2_SCALE)
            )
            loc = cmn.LatLon(total_lat / n, total_lon / n)

            all_destinations = list(all_destinations)
            if len(all_destinations):
                [(top_destination, top_count)] = Counter(all_destinations).most_common(1)
            else:
                top_destination = ""

            return AnchorageLocation(
                mean_location=loc,
                total_visits=n,
                vessels=frozenset(vessels),
                fishing_vessels=frozenset(fishing_vessels),
                rms_drift_radius=math.sqrt(total_squared_drift_radius / n),
                top_destination=top_destination,
                s2id=s2id,
                neighbor_s2ids=neighbor_s2ids,
                active_ssvids=active_ssvid_count,
                total_ssvids=total_ssvid_count,
                stationary_ssvid_days=stationary_days,
                stationary_fishing_ssvid_days=stationary_fishing_days,
                active_ssvid_days=active_days,
            )
        else:
            return None
