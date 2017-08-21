import pytest
import json
import datetime
import anchorages
import pickle
import s2sphere


example_data = """
{"slot_timeout":2,"status":15,"tagblock_T":"2016-01-01 05.20.13","measure_courseavg_43200":0.4990476245,"measure_cos_course":-0.0345420411,"maneuver":0,"scaled":true,"course":92.8000030518,"second":10,"measure_sin_course":0.706262591,"measure_speed":0.0647059048,"speed":15.8999996185,"gridcode":"320023303001301","measure_latavg_43200":55.7151729039,"tagblock_q":"u","tagblock_timestamp":"2016-01-01T05:20:13.000000Z","lc":"good","measure_new_score":0.0,"lon":9.2907962799,"measure_coursestddev_43200_log":-0.2078069473,"lat":55.2189674377,"measure_distance_from_port":1.0,"type":1,"measure_course":0.2577777863,"accuracy":false,"measure_pos_43200":0.1324934886,"distance_from_shore":0.0,"repeat":0,"sync_state":0,"timestamp":"2016-01-01T05:20:13.000000Z","mmsi":111219501,"measure_speedavg_43200":0.1193277335,"raim":true,"spare":0,"slot_number":421,"device":"stdin","distance_from_port":23769.14453125,"measure_coursestddev_43200":0.6187164904,"class":"AIS","measure_speedstddev_43200_log":-0.5721704819,"measure_speedstddev_43200":0.2668116826,"tagblock_station":"rORBCOMM000","measure_heading":1.4194444444,"turn":null,"seg_id":"111219501-2015-12-31T18:22:41.000000Z","measure_lonavg_43200":9.0403326579,"status_text":"Not defined","heading":511}
{"tagblock_group":{"sentence":1,"id":2122,"groupsize":2},"tagblock_T":"2016-01-01 05.21.13","to_port":3,"to_bow":17,"scaled":true,"tagblock_station":"rORBCOMM000","callsign":"RESCUE","shiptype":51,"to_starboard":3,"tagblock_timestamp":"2016-01-01T05:21:13.000000Z","lc":null,"ais_version":1,"destination":"","imo":0,"shiptype_text":"Search and Rescue vessel","type":5,"repeat":0,"draught":0.0,"timestamp":"2016-01-01T05:21:13.000000Z","mmsi":111219501,"epfd":1,"spare":0,"device":"stdin","class":"AIS","to_stern":5,"shipname":"RDAF-SAR1","epfd_text":"GPS","dte":1,"seg_id":"111219501-2015-12-31T18:22:41.000000Z"}
{"slot_timeout":3,"status":15,"tagblock_T":"2016-01-01 05.22.26","measure_courseavg_43200":0.4417013937,"measure_cos_course":0.6845837617,"maneuver":0,"scaled":true,"course":14.5,"second":22,"measure_sin_course":0.1770453987,"measure_speed":0.0,"speed":102.1999969482,"gridcode":"320023303012033","measure_latavg_43200":55.6583690643,"tagblock_q":"u","tagblock_timestamp":"2016-01-01T05:22:26.000000Z","lc":"good","measure_new_score":0.0,"lon":9.3602828979,"measure_coursestddev_43200_log":-0.1720356351,"lat":55.2607421875,"measure_distance_from_port":1.0,"type":1,"measure_course":0.0402777778,"accuracy":false,"measure_pos_43200":0.1352132962,"distance_from_shore":0.0,"repeat":0,"sync_state":0,"timestamp":"2016-01-01T05:22:26.000000Z","mmsi":111219501,"measure_speedavg_43200":0.1044117668,"raim":true,"spare":0,"device":"stdin","distance_from_port":15132.3740234375,"measure_coursestddev_43200":0.6719214389,"class":"AIS","received_stations":9228,"measure_speedstddev_43200_log":-0.5957133144,"measure_speedstddev_43200":0.2526802667,"tagblock_station":"rORBCOMM000","measure_heading":1.4194444444,"turn":null,"seg_id":"111219501-2015-12-31T18:22:41.000000Z","measure_lonavg_43200":9.0803264379,"status_text":"Not defined","heading":511}
{"slot_timeout":3,"status":15,"tagblock_T":"2016-01-01 05.22.49","measure_courseavg_43200":0.3966358067,"measure_cos_course":0.6889836802,"maneuver":0,"scaled":true,"course":13.0,"second":48,"measure_sin_course":0.159064416,"measure_speed":0.0,"speed":102.1999969482,"gridcode":"320023303012322","measure_latavg_43200":55.6162308587,"tagblock_q":"u","tagblock_timestamp":"2016-01-01T05:22:49.000000Z","lc":"good","measure_new_score":0.0,"lon":9.3681402206,"measure_coursestddev_43200_log":-0.1577271347,"lat":55.2791252136,"measure_distance_from_port":1.0,"type":1,"measure_course":0.0361111111,"accuracy":false,"measure_pos_43200":0.1348620573,"distance_from_shore":0.0,"repeat":0,"sync_state":0,"timestamp":"2016-01-01T05:22:49.000000Z","mmsi":111219501,"measure_speedavg_43200":0.0928104594,"raim":true,"spare":0,"device":"stdin","distance_from_port":15811.0,"measure_coursestddev_43200":0.6944611355,"class":"AIS","received_stations":9228,"measure_speedstddev_43200_log":-0.6171215884,"measure_speedstddev_43200":0.2404784679,"tagblock_station":"rORBCOMM000","measure_heading":1.4194444444,"turn":null,"seg_id":"111219501-2015-12-31T18:22:41.000000Z","measure_lonavg_43200":9.1123057471,"status_text":"Not defined","heading":511}
{"tagblock_group":{"sentence":1,"id":7356,"groupsize":2},"tagblock_T":"2016-01-01 05.27.13","to_port":3,"to_bow":17,"scaled":true,"tagblock_station":"rORBCOMM000","callsign":"RESCUE","shiptype":51,"to_starboard":3,"tagblock_timestamp":"2016-01-01T05:27:13.000000Z","lc":null,"ais_version":1,"destination":"","imo":0,"shiptype_text":"Search and Rescue vessel","type":5,"repeat":0,"draught":0.0,"timestamp":"2016-01-01T05:27:13.000000Z","mmsi":111219501,"epfd":1,"spare":0,"device":"stdin","class":"AIS","to_stern":5,"shipname":"RDAF-SAR1","epfd_text":"GPS","dte":1,"seg_id":"111219501-2015-12-31T18:22:41.000000Z"}
""".strip()

examples_msgs = [json.loads(x) for x in example_data.split("\n")]

# Sabotage example message so that we can see some bad messages later
examples_msgs[3]['lat'] = 361

example_records = [anchorages.Records_from_msg(x, [])[0][1] for x in examples_msgs if anchorages.is_location_message(x)]
example_records.append


def test_is_location_message():
    assert [anchorages.is_location_message(x) for x in examples_msgs] == [1, 0, 1, 1, 0]

# TODO: make factor function for creating bad values (override one at a time)
# and then test more cases here
def test_is_not_bad_value():
    assert [anchorages.is_not_bad_value(x) for x in example_records] == [True, True, False]


class TestVesselLocationRecord(object):

    def test_create(self):
        [(md, obj)] = anchorages.Records_from_msg(examples_msgs[0], [])
        print(obj)
        assert obj == anchorages.VesselLocationRecord(
                    timestamp=datetime.datetime(2016, 1, 1, 5, 20, 13), 
                    location=anchorages.LatLon(lat=55.2189674377, lon=9.2907962799), 
                    distance_from_shore=0.0, 
                    speed=15.9, 
                    course=92.8000030518)

    def test_pickle(self):
        [(md, obj)] = anchorages.Records_from_msg(examples_msgs[0], [])
        assert pickle.loads(pickle.dumps(obj)) == obj


class TestVesselMetadata(object):

    def test_create(self):
        assert [anchorages.VesselMetadata_from_msg(x) for x in examples_msgs[:2]] == [
                            anchorages.VesselMetadata(mmsi=111219501), anchorages.VesselMetadata(mmsi=111219501)]

    def test_pickle(self):
        obj = anchorages.VesselMetadata_from_msg(examples_msgs[0])
        assert pickle.loads(pickle.dumps(obj)) == obj


locations = {
    'New York': anchorages.LatLon(40.7128, -74.0059),
    "Chicago": anchorages.LatLon(41.8781, -87.6298),
    "Los Angeles": anchorages.LatLon(34.0522, -118.2437),
    "Phoenix": anchorages.LatLon(33.4484, -112.0740),
    "Scottsdale": anchorages.LatLon(33.4942, -111.9261),
    "Tokyo": anchorages.LatLon(35.6895, 139.6917),
    "Ocean-1": anchorages.LatLon(4, -153),
    "Ocean-2": anchorages.LatLon(0, 179),
    "Ocean-3": anchorages.LatLon(-29, 77),
}

inland_locations = {
    "Chicago": 2336.340987950822,
    "Los Angeles": 574.265826359301,
    "Phoenix": 0,
    "Scottsdale": 14.633197815695059,
}


class TestMask(object):

    def test_locations(self):
        for key in sorted(locations):
            is_inland  = key in inland_locations
            assert anchorages.inland_mask.query(locations[key]) == is_inland, (key, locations[key], is_inland)


class TestAnchoragePoints(object):

    @classmethod
    def LatLon_from_S2Token(cls, token):
        s2id = s2sphere.CellId.from_token(unicode(token))
        s2latlon = s2id.to_lat_lng()
        return anchorages.LatLon(s2latlon.lat().degrees, s2latlon.lng().degrees)

    @classmethod
    def AnchoragePoint_from_S2Token(cls, token, mmsis, total_visits=10, mean_distance_from_shore=0, mean_drift_radius=0, 
                                    top_destinations=()):
        return anchorages.AnchoragePoint(cls.LatLon_from_S2Token(token),
                                         total_visits,
                                         tuple(anchorages.VesselMetadata(x) for x in mmsis),
                                         mean_distance_from_shore,
                                         mean_drift_radius,
                                         top_destinations,
                                         unicode(token),
                                         0,
                                         0)


    def test_to_json(self):
        ap = self.AnchoragePoint_from_S2Token("89c19c9c",
                                            (1, 2, 3))
        assert json.loads(anchorages.anchorage_point_to_json(ap)) == {'destinations': [], 
                                    'lat': 39.9950354853, 'lon': -74.129868411,
                                     'total_visits': 10, 'unique_active_mmsi': 0, 
                                     'drift_radius': 0,
                                     'unique_stationary_mmsi': 3,
                                     'unique_total_mmsi': 0}

        ap = self.AnchoragePoint_from_S2Token("89c19c9c",
                                            (1, 2, 3, 4),
                                            total_visits=17,
                                            top_destinations=('here', 'there'))
        assert json.loads(anchorages.anchorage_point_to_json(ap)) == {'destinations': ['here', 'there'], 
                                    'lat': 39.9950354853, 'lon': -74.129868411,
                                    'total_visits': 17, 
                                    'drift_radius': 0,
                                    'unique_active_mmsi': 0,
                                    'unique_stationary_mmsi': 4,
                                    'unique_total_mmsi': 0}


class TestAnchorages(object):


    @property
    def anchorage_pts(self):
        return [
              TestAnchoragePoints.AnchoragePoint_from_S2Token("89c19c9c",
                                            (1, 2, 3),
                                            top_destinations=(('HERE', 3), ('THERE', 4))),
              TestAnchoragePoints.AnchoragePoint_from_S2Token("89c19b64", (1, 2),
                                            top_destinations=(('THERE', 3), ('EVERYWHERE', 10))),
              TestAnchoragePoints.AnchoragePoint_from_S2Token("89c1852c", [1]),
              TestAnchoragePoints.AnchoragePoint_from_S2Token("89c19b04",
                                            (1, 2),
                                            20,
                                            30.0),
              TestAnchoragePoints.AnchoragePoint_from_S2Token("89c19bac",
                                            (7, 8),
                                            13,
                                            20.0),
              TestAnchoragePoints.AnchoragePoint_from_S2Token("89c19bb4",
                                            [1, 2],
                                            8,
                                            10.0)
        ]   


    def test_merge(self):
        anchs = self.anchorage_pts
 
        grouped_anchorages = anchorages.merge_adjacent_anchorage_points(anchs)
 
        assert len(grouped_anchorages) == 3
 
        expected = [sorted([anchs[2]]), 
                    sorted([anchs[0], anchs[1]]), 
                    sorted([anchs[3], anchs[4], anchs[5]])]
 
        assert sorted(grouped_anchorages) == sorted(expected)
 




    def test_to_json(self):
        assert json.loads(anchorages.Anchorages.to_json(self.anchorage_pts)) == {
                        'id': '89c19b0c',
                        'lat': 39.9832701713,
                        'lon': -74.0995242524,
                        'total_visits': 71,
                        'unique_mmsi': 5,
                        'destinations': [['EVERYWHERE', 10], ['THERE', 7], ['HERE', 3]]}




# class TestUnionFind(object):
#     def test_merge(self):
#         uf1 = anchorages.UnionFind()
#         uf1.union(1, 2)
#         uf1.union(3, 4)
#         uf2 = anchorages.UnionFind()
#         uf2.union(4, 5)
#         uf2.union(6, 7)
#         uf3 = anchorages.UnionFind()
#         uf3.union(0, 1)
#         uf3.union(9, 10)

#         uf1.merge(uf2, uf3)

#         assert sorted(uf1.parents.items()) == [(0, 2), (1, 2), (2, 2), (3, 4), (4, 4), 
#                                                 (5, 4), (6, 7), (7, 7), (9, 10), (10, 10)]


class TestUtilities(object):
    distances = {
        'New York': 3443.706085594739,
        "Chicago": 2336.340987950822,
        "Los Angeles": 574.265826359301,
        "Phoenix": 0,
        "Scottsdale": 14.633197815695059,
        "Tokyo": 9308.45399157672,
    }

    def test_distances(self):
        phx = locations['Phoenix']

        for key in sorted(locations):
            if key.startswith("Ocean"):
                continue
            assert anchorages.distance(phx, locations[key]) == self.distances[key], (key, anchorages.distance(phx, locations[key]),  
                self.distances[key])

