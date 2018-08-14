from collections import OrderedDict
import datetime
import numpy as np
import pytest
import pytz

from pipe_anchorages.objects.visit_event import VisitEvent
from pipe_anchorages.port_visits_pipeline import visit_to_msg
from pipe_anchorages.transforms.create_port_visits import CreatePortVisits


expected_1 = OrderedDict([('vessel_id', '412424227'), ('start_timestamp', 1476426402.0), 
    ('start_lat', 29.9667462525), ('start_lon', 122.4396281067), ('start_anchorage_id', u'345328af'), 
    ('end_timestamp', 1476495140.0), ('end_lat', 29.9667462525), ('end_lon', 122.4396281067), 
    ('end_anchorage_id', u'345328af'), ('events', [
        OrderedDict([('anchorage_id', u'345328af'), 
            ('lat', 29.9667462525), ('lon', 122.4396281067), ('vessel_lat', 29.9806137085), 
            ('vessel_lon', 122.4564437866), ('vessel_id', 412424227), ('timestamp', 1476426402.0), 
            ('event_type', u'PORT_ENTRY')]), 
        OrderedDict([('anchorage_id', u'3452d969'), ('lat', 29.9407869352), ('lon', 122.27906699), 
            ('vessel_lat', 29.9408073425), ('vessel_lon', 122.2787628174), ('vessel_id', 412424227), 
            ('timestamp', 1476430743.0), ('event_type', u'PORT_STOP_BEGIN')]), 
        OrderedDict([('anchorage_id', u'3452d95d'), 
            ('lat', 29.9399344611), ('lon', 122.2747471358), ('vessel_lat', 29.9394741058), 
            ('vessel_lon', 122.2749481201), ('vessel_id', 412424227), ('timestamp', 1476436086.0), 
            ('event_type', u'PORT_STOP_END')]), 
        OrderedDict([('anchorage_id', u'3452d95f'), ('lat', 29.941832869), 
            ('lon', 122.2689838563), ('vessel_lat', 29.9417858124), 
            ('vessel_lon', 122.2697219849), ('vessel_id', 412424227), 
            ('timestamp', 1476488638.0), ('event_type', u'PORT_STOP_BEGIN')]), 
        OrderedDict([('anchorage_id', u'3452d95d'), 
            ('lat', 29.9399344611), ('lon', 122.2747471358), ('vessel_lat', 29.9404067993), 
            ('vessel_lon', 122.2765045166), ('vessel_id', 412424227), ('timestamp', 1476489542.0), 
            ('event_type', u'PORT_STOP_END')]), 
        OrderedDict([('anchorage_id', u'345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067), 
            ('vessel_lat', 30.0182361603), ('vessel_lon', 122.475944519), ('vessel_id', 412424227), 
            ('timestamp', 1476495140.0), ('event_type', u'PORT_EXIT')])])])


expected_2 = OrderedDict([('vessel_id', '273386660'), ('start_timestamp', 1475241790.0), 
    ('start_lat', 45.7285312075), ('start_lon', 47.6336454619), ('start_anchorage_id', u'41abf04b'), 
    ('end_timestamp', 1475290556.0), ('end_lat', 46.3122421582), ('end_lon', 47.9769367751), 
    ('end_anchorage_id', u'41a90e45'), ('events', [
        OrderedDict([('anchorage_id', u'41abf04b'), ('lat', 45.7285312075), ('lon', 47.6336454619), 
            ('vessel_lat', 45.7202682495), ('vessel_lon', 47.6396331787), ('vessel_id', 273386660), 
            ('timestamp', 1475241790.0), ('event_type', u'PORT_ENTRY')]), 
        OrderedDict([('anchorage_id', u'41a90fa9'), ('lat', 46.3280197425), ('lon', 47.9900577312), 
            ('vessel_lat', 46.3271255493), ('vessel_lon', 47.9901542664), ('vessel_id', 273386660), 
            ('timestamp', 1475256506.0),  ('event_type', u'PORT_STOP_BEGIN')]), 
        OrderedDict([('anchorage_id', u'41a90f75'), 
            ('lat', 46.3406197011), ('lon', 48.0045788836), ('vessel_lat', 46.3408584595), 
            ('vessel_lon', 48.0045700073), ('vessel_id', 273386660), ('timestamp', 1475258350.0), 
            ('event_type', u'PORT_STOP_END')]), 
        OrderedDict([('anchorage_id', u'41a90f75'), ('lat', 46.3406197011), ('lon', 48.0045788836), 
            ('vessel_lat', 46.3400001526), ('vessel_lon', 48.0033340454), ('vessel_id', 273386660), 
            ('timestamp', 1475260293.0), ('event_type', u'PORT_STOP_BEGIN')]), 
        OrderedDict([('anchorage_id', u'41a90e45'), ('lat', 46.3122421582), ('lon', 47.9769367751), 
            ('vessel_lat', 46.3120269775), ('vessel_lon', 47.9736251831), ('vessel_id', 273386660), 
            ('timestamp', 1475287217.0), ('event_type', u'PORT_STOP_END')]), 
        OrderedDict([('anchorage_id', u'41a90e45'), ('lat', 46.3122421582), ('lon', 47.9769367751), 
            ('vessel_lat', 46.1234703064), ('vessel_lon', 47.7903404236), ('vessel_id', 273386660), 
            ('timestamp', 1475290556.0), ('event_type', u'PORT_EXIT')])])])


# Add some out of order stuff to expected_1. The stuff before port_entry at events[5] should be dropped
events_3 = [
        OrderedDict([('anchorage_id', u'3452d95f'), ('lat', 29.941832869), 
            ('lon', 122.2689838563), ('vessel_lat', 29.9417858124), 
            ('vessel_lon', 122.2697219849), ('vessel_id', 412424227), 
            ('timestamp', 1476426399.0), ('event_type', u'PORT_STOP_BEGIN')]), 
        OrderedDict([('anchorage_id', u'3452d95d'), 
            ('lat', 29.9399344611), ('lon', 122.2747471358), ('vessel_lat', 29.9404067993), 
            ('vessel_lon', 122.2765045166), ('vessel_id', 412424227), ('timestamp', 1476426400.0), 
            ('event_type', u'PORT_STOP_END')]), 
        OrderedDict([('anchorage_id', u'345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067), 
            ('vessel_lat', 30.0182361603), ('vessel_lon', 122.475944519), ('vessel_id', 412424227), 
            ('timestamp', 1476426401.0), ('event_type', u'PORT_EXIT')]),
        OrderedDict([('anchorage_id', u'345328af'), 
            ('lat', 29.9667462525), ('lon', 122.4396281067), ('vessel_lat', 29.9806137085), 
            ('vessel_lon', 122.4564437866), ('vessel_id', 412424227), ('timestamp', 1476426401.8), 
            ('event_type', u'PORT_ENTRY')]), 
        OrderedDict([('anchorage_id', u'3452d969'), ('lat', 29.9407869352), ('lon', 122.27906699), 
            ('vessel_lat', 29.9408073425), ('vessel_lon', 122.2787628174), ('vessel_id', 412424227), 
            ('timestamp', 1476426401.9), ('event_type', u'PORT_STOP_BEGIN')]),
        OrderedDict([('anchorage_id', u'345328af'), 
            ('lat', 29.9667462525), ('lon', 122.4396281067), ('vessel_lat', 29.9806137085), 
            ('vessel_lon', 122.4564437866), ('vessel_id', 412424227), ('timestamp', 1476426402.0), 
            ('event_type', u'PORT_ENTRY')]), 
        OrderedDict([('anchorage_id', u'3452d969'), ('lat', 29.9407869352), ('lon', 122.27906699), 
            ('vessel_lat', 29.9408073425), ('vessel_lon', 122.2787628174), ('vessel_id', 412424227), 
            ('timestamp', 1476430743.0), ('event_type', u'PORT_STOP_BEGIN')]), 
        OrderedDict([('anchorage_id', u'3452d95d'), 
            ('lat', 29.9399344611), ('lon', 122.2747471358), ('vessel_lat', 29.9394741058), 
            ('vessel_lon', 122.2749481201), ('vessel_id', 412424227), ('timestamp', 1476436086.0), 
            ('event_type', u'PORT_STOP_END')]), 
        OrderedDict([('anchorage_id', u'3452d95f'), ('lat', 29.941832869), 
            ('lon', 122.2689838563), ('vessel_lat', 29.9417858124), 
            ('vessel_lon', 122.2697219849), ('vessel_id', 412424227), 
            ('timestamp', 1476488638.0), ('event_type', u'PORT_STOP_BEGIN')]), 
        OrderedDict([('anchorage_id', u'3452d95d'), 
            ('lat', 29.9399344611), ('lon', 122.2747471358), ('vessel_lat', 29.9404067993), 
            ('vessel_lon', 122.2765045166), ('vessel_id', 412424227), ('timestamp', 1476489542.0), 
            ('event_type', u'PORT_STOP_END')]), 
        OrderedDict([('anchorage_id', u'345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067), 
            ('vessel_lat', 30.0182361603), ('vessel_lon', 122.475944519), ('vessel_id', 412424227), 
            ('timestamp', 1476495140.0), ('event_type', u'PORT_EXIT')])]



expected = [(expected_1['events'], [expected_1]), 
            (expected_2['events'], [expected_2]),
            (events_3,             [expected_1]),
            # Since time for these two don't overlap, we can use these even though 
            # vessel ids are different
            (expected_2['events'] + events_3, [expected_2, expected_1])
            ]


def evt_from_dict(x):
    x['timestamp'] = datetime.datetime.utcfromtimestamp(x['timestamp']).replace(tzinfo=pytz.utc)
    return VisitEvent(**x)


class TestCreatePortVisits(object):

    permutations_per_case = 3

    def generate_test_cases(self):
        np.random.seed(4321)
        for raw_evts, exp in expected:
            evts = [evt_from_dict(x.copy()) for x in raw_evts]
            for i in range(self.permutations_per_case):
                np.random.shuffle(evts)
                yield (evts, exp)



    def test_test_cases(self):
        # Ensure that evts are really getting scrambled
        orders = []
        for (events, _) in self.generate_test_cases():
            orders.append([x.event_type for x in events])
        assert orders[:6] == [['PORT_STOP_BEGIN',
                           'PORT_STOP_BEGIN',
                           'PORT_STOP_END',
                           'PORT_ENTRY',
                           'PORT_STOP_END',
                           'PORT_EXIT'],
                          ['PORT_STOP_BEGIN',
                           'PORT_EXIT',
                           'PORT_STOP_END',
                           'PORT_STOP_BEGIN',
                           'PORT_ENTRY',
                           'PORT_STOP_END'],
                          ['PORT_STOP_BEGIN',
                           'PORT_STOP_BEGIN',
                           'PORT_EXIT',
                           'PORT_STOP_END',
                           'PORT_ENTRY',
                           'PORT_STOP_END'],
                          ['PORT_STOP_BEGIN',
                           'PORT_STOP_END',
                           'PORT_STOP_END',
                           'PORT_ENTRY',
                           'PORT_STOP_BEGIN',
                           'PORT_EXIT'],
                          ['PORT_STOP_END',
                           'PORT_STOP_BEGIN',
                           'PORT_STOP_BEGIN',
                           'PORT_STOP_END',
                           'PORT_ENTRY',
                           'PORT_EXIT'],
                          ['PORT_STOP_END',
                           'PORT_STOP_END',
                           'PORT_STOP_BEGIN',
                           'PORT_ENTRY',
                           'PORT_EXIT',
                           'PORT_STOP_BEGIN']]


    def test_creation(self):
        target = CreatePortVisits()
        for events, expected in self.generate_test_cases():
            result = target.create_port_visits((None, events))
            assert [visit_to_msg(x) for x in result] == expected
