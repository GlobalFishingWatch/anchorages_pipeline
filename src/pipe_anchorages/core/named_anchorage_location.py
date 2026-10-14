from collections import namedtuple

from pipe_anchorages import common as cmn
from pipe_anchorages.core.anchorage_location import AnchorageLocation


class NamedAnchorageLocation(
    namedtuple(
        "NamedAnchorageLocation",
        AnchorageLocation._fields + ("label", "sublabel", "iso3", "label_source"),
    )
):

    """An AnchorageLocation with a port name (label), an anchorage name (sublabel), the ISO3 of
    its EEZ, and where the name comes from (label_source).

    Read back from the anchorage locations table, it has no vessel sets: `vessels` and
    `fishing_vessels` are lists of None, as long as the counts they come from.
    """

    __slots__ = ()

    # No type hints: Beam maps it, and would infer coders from them.
    @staticmethod
    def from_msg(msg):
        """Builds a NamedAnchorageLocation, with no names yet, from an anchorage locations row."""
        msg["mean_location"] = cmn.LatLon(msg.pop("lat"), msg.pop("lon"))
        msg["rms_drift_radius"] = msg.pop("drift_radius")
        msg["vessels"] = [None] * msg.pop("unique_stationary_ssvid")
        msg["fishing_vessels"] = [None] * msg.pop("unique_stationary_fishing_ssvid")
        msg["active_ssvids"] = msg.pop("unique_active_ssvid")
        msg["total_ssvids"] = msg.pop("unique_total_ssvid")
        msg["stationary_fishing_ssvid_days"] = msg.pop("stationary_fishing_ssvid_days")
        msg["stationary_ssvid_days"] = msg.pop("stationary_ssvid_days")
        msg["active_ssvid_days"] = msg.pop("active_ssvid_days")
        msg["label"] = None
        msg["sublabel"] = None
        msg["iso3"] = None
        msg["label_source"] = None
        msg["neighbor_s2ids"] = None

        return NamedAnchorageLocation(**msg)
