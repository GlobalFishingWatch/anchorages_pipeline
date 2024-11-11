import os
import pytest
import json
import datetime
import pickle
import six
import s2sphere

from pipe_anchorages import find_anchorage_points
from pipe_anchorages import common
from pipe_anchorages import nearest_port
from pipe_anchorages.records import is_location_message, has_valid_location
from pipe_anchorages.records import VesselRecord
from pipe_anchorages.records import VesselLocationRecord
from pipe_anchorages.transforms import sink


class TestAnchorageSink(object):

    @classmethod
    def LatLon_from_S2Token(cls, token):
        s2id = s2sphere.CellId.from_token(six.text_type(token))
        s2latlon = s2id.to_lat_lng()
        return common.LatLon(s2latlon.lat().degrees, s2latlon.lng().degrees)

    @classmethod
    def AnchoragePoint_from_S2Token(
        cls, token, ssvids, total_visits=10, mean_drift_radius=0.2, top_destination=""
    ):
        token = six.text_type(token)
        return find_anchorage_points.AnchoragePoint(
            mean_location=cls.LatLon_from_S2Token(token),
            total_visits=total_visits,
            vessels=tuple(ssvids),
            fishing_vessels=tuple(ssvids[:2]),
            rms_drift_radius=mean_drift_radius,
            top_destination=top_destination,
            s2id=token,
            neighbor_s2ids=tuple(
                s2sphere.CellId.from_token(token).get_all_neighbors(common.ANCHORAGES_S2_SCALE)
            ),
            active_ssvids=2,
            total_ssvids=0,
            stationary_ssvid_days=4.1,
            stationary_fishing_ssvid_days=3.3,
            active_ssvid_days=7.2,
        )

    def test_encoder(self):
        anchorage = self.AnchoragePoint_from_S2Token("0d1b968b", [37, 49, 2])

        asink = sink.AnchorageSink(None, None, None)
        encoded = asink.encode(anchorage)

        assert len(encoded) == len(asink.spec)

        type_map = {int: "integer", str: "string", six.text_type: "string", float: "float"}

        for k, v in encoded.items():
            t, desc = asink.spec[k]
            assert type_map[type(v)] == t, (k, type(v), t)
