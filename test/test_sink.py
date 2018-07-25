import os
import pytest
import json
import datetime
import pickle
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
        s2id = s2sphere.CellId.from_token(unicode(token))
        s2latlon = s2id.to_lat_lng()
        return common.LatLon(s2latlon.lat().degrees, s2latlon.lng().degrees)

    @classmethod
    def AnchoragePoint_from_S2Token(cls, token, vessel_ids, total_visits=10, mean_drift_radius=0.2, 
                                    top_destination=''):
        token = unicode(token)
        return find_anchorage_points.AnchoragePoint(
                                mean_location = cls.LatLon_from_S2Token(token),
                                total_visits = total_visits,
                                vessels = tuple(vessel_ids),
                                fishing_vessels = tuple(vessel_ids[:2]),
                                rms_drift_radius = mean_drift_radius,
                                top_destination = top_destination,
                                s2id = token,
                                neighbor_s2ids = tuple(s2sphere.CellId.from_token(token).get_all_neighbors(common.ANCHORAGES_S2_SCALE)),
                                active_vessel_ids = 2,
                                total_vessel_ids = 0,
                                stationary_vessel_id_days = 4.1,
                                stationary_fishing_vessel_id_days = 3.3,
                                active_vessel_id_days = 7.2,
                                )



    def test_encoder(self):
        anchorage = self.AnchoragePoint_from_S2Token('0d1b968b', [37, 49, 2])

        asink = sink.AnchorageSink(None, None)
        encoded = asink.encode(anchorage)

        assert len(encoded) == len(asink.spec)

        type_map = {
            int : 'integer',
            str : 'string',
            unicode : 'string',
            float : 'float'}

        for k, v in encoded.items():
            assert type_map[type(v)] == asink.spec[k], (k, type(v), asink.spec[k])

