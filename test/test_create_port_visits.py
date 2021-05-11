from collections import OrderedDict
import datetime
import numpy as np
import pytest
import pytz

from pipe_anchorages.objects.visit_event import VisitEvent
from pipe_anchorages.port_visits_pipeline import visit_to_msg
from pipe_anchorages.transforms.create_port_visits import CreatePortVisits


expected_1 = OrderedDict([('visit_id', '983f3328bed78676306504f0df69e75e'), ('ssvid', 'None'), ('vessel_id', 'None'), 
    ('start_timestamp', 1476426402.0), ('start_lat', 29.9667462525), ('start_lon', 122.4396281067), 
    ('start_anchorage_id', '345328af'), ('end_timestamp', 1476495140.0), ('end_lat', 29.9667462525), 
    ('end_lon', 122.4396281067), ('end_anchorage_id', '345328af'), ('duration_hrs', 19.093888888888888), 
    ('events', [
        OrderedDict([('anchorage_id', '345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067),
            ('vessel_lat', 29.9806137085), ('vessel_lon', 122.4564437866), ('seg_id', 412424227), 
            ('timestamp', 1476426402.0), ('event_type', 'PORT_ENTRY'), ('last_timestamp', 1476426302.0)]), 
        OrderedDict([('anchorage_id', '3452d969'), ('lat', 29.9407869352), ('lon', 122.27906699), 
            ('vessel_lat', 29.9408073425), ('vessel_lon', 122.2787628174), ('seg_id', 412424227), 
            ('timestamp', 1476430743.0), ('event_type', 'PORT_STOP_BEGIN'), ('last_timestamp', 1476430643.0)]), 
        OrderedDict([('anchorage_id', '3452d95d'), ('lat', 29.9399344611), ('lon', 122.2747471358), 
            ('vessel_lat', 29.9394741058), ('vessel_lon', 122.2749481201), ('seg_id', 412424227), 
            ('timestamp', 1476436086.0), ('event_type', 'PORT_STOP_END'), ('last_timestamp', 1476436006.0)]), 
        OrderedDict([('anchorage_id', '3452d95f'), ('lat', 29.941832869), ('lon', 122.2689838563), 
            ('vessel_lat', 29.9417858124), ('vessel_lon', 122.2697219849), ('seg_id', 412424227), 
            ('timestamp', 1476488638.0), ('event_type', 'PORT_STOP_BEGIN'), ('last_timestamp', 1476436086.0)]), 
        OrderedDict([('anchorage_id', '3452d95d'), ('lat', 29.9399344611), ('lon', 122.2747471358),
            ('vessel_lat', 29.9404067993), ('vessel_lon', 122.2765045166), ('seg_id', 412424227), 
            ('timestamp', 1476489542.0), ('event_type', 'PORT_STOP_END'), ('last_timestamp', 1476436086.0)]), 
        OrderedDict([('anchorage_id', '345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067), 
            ('vessel_lat', 30.0182361603), ('vessel_lon', 122.475944519), ('seg_id', 412424227), 
            ('timestamp', 1476495140.0), ('event_type', 'PORT_EXIT'), ('last_timestamp', 1476436086.0)])]),
    ('confidence', 4)])


expected_2 = OrderedDict([('visit_id', '4d00bc906664b0d659e9971f45482790'), ('ssvid', 'None'), ('vessel_id', 'None'), 
    ('start_timestamp', 1475241790.0), ('start_lat', 45.7285312075), ('start_lon', 47.6336454619),
    ('start_anchorage_id', '41abf04b'), ('end_timestamp', 1475290556.0), ('end_lat', 46.3122421582), ('end_lon', 47.9769367751), 
    ('end_anchorage_id', '41a90e45'), ('duration_hrs', 13.546111111111111), ('events', [
        OrderedDict([('anchorage_id', '41abf04b'), ('lat', 45.7285312075), ('lon', 47.6336454619), 
            ('vessel_lat', 45.7202682495), ('vessel_lon', 47.6396331787), ('seg_id', 273386660), ('timestamp', 1475241790.0), 
            ('event_type', 'PORT_ENTRY'), ('last_timestamp', 1476426302.0)]), 
        OrderedDict([('anchorage_id', '41a90fa9'), ('lat', 46.3280197425), ('lon', 47.9900577312), 
            ('vessel_lat', 46.3271255493), ('vessel_lon', 47.9901542664), ('seg_id', 273386660), ('timestamp', 1475256506.0), 
            ('event_type', 'PORT_STOP_BEGIN'), ('last_timestamp', 1476426302.0)]), 
        OrderedDict([('anchorage_id', '41a90f75'), ('lat', 46.3406197011), ('lon', 48.0045788836), 
            ('vessel_lat', 46.3408584595), ('vessel_lon', 48.0045700073), ('seg_id', 273386660), ('timestamp', 1475258350.0), 
            ('event_type', 'PORT_STOP_END'), ('last_timestamp', 1476426302.0)]), 
        OrderedDict([('anchorage_id', '41a90f75'), ('lat', 46.3406197011), ('lon', 48.0045788836), 
            ('vessel_lat', 46.3400001526), ('vessel_lon', 48.0033340454), ('seg_id', 273386660), ('timestamp', 1475260293.0), 
            ('event_type', 'PORT_STOP_BEGIN'), ('last_timestamp', 1476426302.0)]), 
        OrderedDict([('anchorage_id', '41a90e45'), ('lat', 46.3122421582), ('lon', 47.9769367751), 
            ('vessel_lat', 46.3120269775), ('vessel_lon', 47.9736251831), ('seg_id', 273386660), ('timestamp', 1475287217.0), 
            ('event_type', 'PORT_STOP_END'), ('last_timestamp', 1476426302.0)]), 
        OrderedDict([('anchorage_id', '41a90e45'), ('lat', 46.3122421582), ('lon', 47.9769367751), 
            ('vessel_lat', 46.1234703064), ('vessel_lon', 47.7903404236), ('seg_id', 273386660), ('timestamp', 1475290556.0), 
            ('event_type', 'PORT_EXIT'), ('last_timestamp', 1476426302.0)])]),
    ('confidence', 4)])
    

expected_3 = [
    OrderedDict([('visit_id', 'ceac3a0dd310b6a6e8c495b5a7509893'), ('ssvid', 'None'), 
        ('vessel_id', 'None'), ('start_timestamp', 1476426399.0), ('start_lat', 29.941832869), ('start_lon', 122.2689838563), 
        ('start_anchorage_id', '3452d95f'), ('end_timestamp', 1476426401.0), ('end_lat', 29.9667462525), 
        ('end_lon', 122.4396281067), ('end_anchorage_id', '345328af'), ('duration_hrs', 0.0005555555555555556), ('events', [
            OrderedDict([('anchorage_id', '3452d95f'), ('lat', 29.941832869), ('lon', 122.2689838563), 
                ('vessel_lat', 29.9417858124), ('vessel_lon', 122.2697219849), ('seg_id', 412424227), 
                ('timestamp', 1476426399.0), ('event_type', 'PORT_STOP_BEGIN'), ('last_timestamp', 1476436086.0)]), 
            OrderedDict([('anchorage_id', '3452d95d'), ('lat', 29.9399344611), ('lon', 122.2747471358), 
                ('vessel_lat', 29.9404067993), ('vessel_lon', 122.2765045166), ('seg_id', 412424227), 
                ('timestamp', 1476426400.0), ('event_type', 'PORT_STOP_END'), ('last_timestamp', 1476436086.0)]), 
            OrderedDict([('anchorage_id', '345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067), 
                ('vessel_lat', 30.0182361603), ('vessel_lon', 122.475944519), ('seg_id', 412424227), 
                ('timestamp', 1476426401.0), ('event_type', 'PORT_EXIT'), ('last_timestamp', 1476436086.0)])]),
        ('confidence', 3)]), 
    OrderedDict([('visit_id', '983f3328bed78676306504f0df69e75e'), ('ssvid', 'None'), ('vessel_id', 'None'), 
        ('start_timestamp', 1476426402.0), ('start_lat', 29.9667462525), ('start_lon', 122.4396281067), 
        ('start_anchorage_id', '345328af'), ('end_timestamp', 1476495140.0), ('end_lat', 29.9667462525), 
        ('end_lon', 122.4396281067), ('end_anchorage_id', '345328af'), ('duration_hrs', 19.093888888888888), ('events', [
            OrderedDict([('anchorage_id', '345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067), 
                ('vessel_lat', 29.9806137085), ('vessel_lon', 122.4564437866), ('seg_id', 412424227), 
                ('timestamp', 1476426402.0), ('event_type', 'PORT_ENTRY'), ('last_timestamp', 1476436086.0)]),
            OrderedDict([('anchorage_id', '3452d969'), ('lat', 29.9407869352), ('lon', 122.27906699), 
                ('vessel_lat', 29.9408073425), ('vessel_lon', 122.2787628174), ('seg_id', 412424227), 
                ('timestamp', 1476430743.0), ('event_type', 'PORT_STOP_BEGIN'), ('last_timestamp', 1476436086.0)]), 
            OrderedDict([('anchorage_id', '3452d95d'), ('lat', 29.9399344611), ('lon', 122.2747471358), 
                ('vessel_lat', 29.9394741058), ('vessel_lon', 122.2749481201), ('seg_id', 412424227), 
                ('timestamp', 1476436086.0), ('event_type', 'PORT_STOP_END'), ('last_timestamp', 1476436086.0)]), 
            OrderedDict([('anchorage_id', '3452d95f'), ('lat', 29.941832869), ('lon', 122.2689838563), 
                ('vessel_lat', 29.9417858124), ('vessel_lon', 122.2697219849), ('seg_id', 412424227), 
                ('timestamp', 1476488638.0), ('event_type', 'PORT_STOP_BEGIN'), ('last_timestamp', 1476436086.0)]), 
            OrderedDict([('anchorage_id', '3452d95d'), ('lat', 29.9399344611), ('lon', 122.2747471358), 
                ('vessel_lat', 29.9404067993), ('vessel_lon', 122.2765045166), ('seg_id', 412424227), 
                ('timestamp', 1476489542.0), ('event_type', 'PORT_STOP_END'), ('last_timestamp', 1476436086.0)]), 
            OrderedDict([('anchorage_id', '345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067), 
                ('vessel_lat', 30.0182361603), ('vessel_lon', 122.475944519), ('seg_id', 412424227), 
                ('timestamp', 1476495140.0), ('event_type', 'PORT_EXIT'), ('last_timestamp', 1476436086.0)])]),
        ('confidence', 4)])]

# Add some out of order stuff to expected_1. 
 #The stuff before port_entry at events[3] becomes an event with no start.

events_3 = [
        OrderedDict([('anchorage_id', u'3452d95f'), ('lat', 29.941832869), ('lon', 122.2689838563),
            ('vessel_lat', 29.9417858124), ('vessel_lon', 122.2697219849), ('seg_id', 412424227),
            ('timestamp', 1476426399.0), ('last_timestamp', 1476436086.0), ('event_type', u'PORT_STOP_BEGIN')]),
        OrderedDict([('anchorage_id', u'3452d95d'), ('lat', 29.9399344611), ('lon', 122.2747471358),
            ('vessel_lat', 29.9404067993), ('vessel_lon', 122.2765045166), ('seg_id', 412424227),
            ('timestamp', 1476426400.0), ('last_timestamp', 1476436086.0), ('event_type', u'PORT_STOP_END')]),
        OrderedDict([('anchorage_id', u'345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067),
            ('vessel_lat', 30.0182361603), ('vessel_lon', 122.475944519), ('seg_id', 412424227),
            ('timestamp', 1476426401.0), ('last_timestamp', 1476436086.0), ('event_type', u'PORT_EXIT')]),
        OrderedDict([('anchorage_id', u'345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067),
            ('vessel_lat', 29.9806137085), ('vessel_lon', 122.4564437866), ('seg_id', 412424227),
            ('timestamp', 1476426402.0), ('last_timestamp', 1476436086.0), ('event_type', u'PORT_ENTRY')]),
        OrderedDict([('anchorage_id', u'3452d969'), ('lat', 29.9407869352), ('lon', 122.27906699),
            ('vessel_lat', 29.9408073425), ('vessel_lon', 122.2787628174), ('seg_id', 412424227),
            ('timestamp', 1476430743.0), ('last_timestamp', 1476436086.0), ('event_type', u'PORT_STOP_BEGIN')]),
        OrderedDict([('anchorage_id', u'3452d95d'), ('lat', 29.9399344611), ('lon', 122.2747471358),
            ('vessel_lat', 29.9394741058), ('vessel_lon', 122.2749481201), ('seg_id', 412424227),
            ('timestamp', 1476436086.0), ('last_timestamp', 1476436086.0), ('event_type', u'PORT_STOP_END')]),
        OrderedDict([('anchorage_id', u'3452d95f'), ('lat', 29.941832869), ('lon', 122.2689838563),
            ('vessel_lat', 29.9417858124), ('vessel_lon', 122.2697219849), ('seg_id', 412424227),
            ('timestamp', 1476488638.0), ('last_timestamp', 1476436086.0), ('event_type', u'PORT_STOP_BEGIN')]),
        OrderedDict([('anchorage_id', u'3452d95d'), ('lat', 29.9399344611), ('lon', 122.2747471358),
            ('vessel_lat', 29.9404067993), ('vessel_lon', 122.2765045166), ('seg_id', 412424227),
            ('timestamp', 1476489542.0), ('last_timestamp', 1476436086.0), ('event_type', u'PORT_STOP_END')]),
        OrderedDict([('anchorage_id', u'345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067),
            ('vessel_lat', 30.0182361603), ('vessel_lon', 122.475944519), ('seg_id', 412424227),
            ('timestamp', 1476495140.0), ('last_timestamp', 1476436086.0), ('event_type', u'PORT_EXIT')])]


# TODO: I think this may be broken because I used random values for last timestamp check XXX
  # Actually not obvious why that would matter, don't think create port_events cares, so why.

  # OOPs looks like I messed up when updating event 2, go back to original and redo.

events_4 = [
    OrderedDict([('visit_id', '983f3328bed78676306504f0df69e75e'), ('ssvid', 'None'), ('vessel_id', 'None'), 
        ('start_timestamp', 1476426402.0), ('start_lat', 29.9667462525), ('start_lon', 122.4396281067), 
        ('start_anchorage_id', '345328af'), ('end_timestamp', 1476495140.0), ('end_lat', 29.9667462525), 
        ('end_lon', 122.4396281067), ('end_anchorage_id', '345328af'), ('duration_hrs', 19.093888888888888), ('events', [
            OrderedDict([('anchorage_id', '345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067), 
                ('vessel_lat', 29.9806137085), ('vessel_lon', 122.4564437866), ('seg_id', 412424227), 
                ('timestamp', 1476426402.0), ('event_type', 'PORT_ENTRY'), ('last_timestamp', 1476426302.0)]), 
            OrderedDict([('anchorage_id', '3452d969'), ('lat', 29.9407869352), ('lon', 122.27906699), 
                ('vessel_lat', 29.9408073425), ('vessel_lon', 122.2787628174), ('seg_id', 412424227), 
                ('timestamp', 1476430743.0), ('event_type', 'PORT_STOP_BEGIN'), ('last_timestamp', 1476430643.0)]), 
            OrderedDict([('anchorage_id', '3452d969'), ('lat', 29.9407869352), ('lon', 122.27906699), 
                ('vessel_lat', 29.9408073425), ('vessel_lon', 122.2787628174), ('seg_id', 412424227), 
                ('timestamp', 1476430743.0), ('event_type', 'PORT_STOP_BEGIN'), ('last_timestamp', 1476430643.0)]), 
            OrderedDict([('anchorage_id', '3452d95d'), ('lat', 29.9399344611), ('lon', 122.2747471358), 
                ('vessel_lat', 29.9394741058), ('vessel_lon', 122.2749481201), ('seg_id', 412424227), 
                ('timestamp', 1476436086.0), ('event_type', 'PORT_STOP_END'), ('last_timestamp', 1476436006.0)]), 
            OrderedDict([('anchorage_id', '3452d95d'), ('lat', 29.9399344611), ('lon', 122.2747471358), 
                ('vessel_lat', 29.9394741058), ('vessel_lon', 122.2749481201), ('seg_id', 412424227), 
                ('timestamp', 1476436086.0), ('event_type', 'PORT_STOP_END'), ('last_timestamp', 1476436006.0)]), 
            OrderedDict([('anchorage_id', '3452d95f'), ('lat', 29.941832869), ('lon', 122.2689838563), 
                ('vessel_lat', 29.9417858124), ('vessel_lon', 122.2697219849), ('seg_id', 412424227), 
                ('timestamp', 1476488638.0), ('event_type', 'PORT_STOP_BEGIN'), ('last_timestamp', 1476436086.0)]), 
            OrderedDict([('anchorage_id', '3452d95f'), ('lat', 29.941832869), ('lon', 122.2689838563), 
                ('vessel_lat', 29.9417858124), ('vessel_lon', 122.2697219849), ('seg_id', 412424227), 
                ('timestamp', 1476488638.0), ('event_type', 'PORT_STOP_BEGIN'), ('last_timestamp', 1476436086.0)]), 
            OrderedDict([('anchorage_id', '3452d95d'), ('lat', 29.9399344611), ('lon', 122.2747471358), 
                ('vessel_lat', 29.9404067993), ('vessel_lon', 122.2765045166), ('seg_id', 412424227), 
                ('timestamp', 1476489542.0), ('event_type', 'PORT_STOP_END'), ('last_timestamp', 1476436086.0)]), 
            OrderedDict([('anchorage_id', '3452d95d'), ('lat', 29.9399344611), ('lon', 122.2747471358), 
                ('vessel_lat', 29.9404067993), ('vessel_lon', 122.2765045166), ('seg_id', 412424227), ('timestamp', 1476489542.0), 
                ('event_type', 'PORT_STOP_END'), ('last_timestamp', 1476436086.0)]), 
            OrderedDict([('anchorage_id', '345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067), 
                ('vessel_lat', 30.0182361603), ('vessel_lon', 122.475944519), ('seg_id', 412424227), 
                ('timestamp', 1476495140.0), ('event_type', 'PORT_EXIT'), ('last_timestamp', 1476436086.0)])])]),
    OrderedDict([('visit_id', '111ea0ee948cdd7b6eb76d8109c36b72'), ('ssvid', 'None'), ('vessel_id', 'None'),
        ('start_timestamp', 1476495140.0), ('start_lat', 29.9667462525), ('start_lon', 122.4396281067), 
        ('start_anchorage_id', '345328af'), ('end_timestamp', 1476495140.0), ('end_lat', 29.9667462525), 
        ('end_lon', 122.4396281067), ('end_anchorage_id', '345328af'), ('duration_hrs', 0.0), ('events', [
            OrderedDict([('anchorage_id', '345328af'), ('lat', 29.9667462525), ('lon', 122.4396281067), 
                ('vessel_lat', 30.0182361603), ('vessel_lon', 122.475944519), ('seg_id', 412424227), 
                ('timestamp', 1476495140.0), ('event_type', 'PORT_EXIT'), ('last_timestamp', 1476436086.0)])])])
]

expected = [
    (expected_1['events'], [expected_1]),
    (expected_2['events'], [expected_2]),
    (events_3,             expected_3),
    # # Since time for these two don't overlap, we can use these even though
    # # vessel ids are different
    (expected_2['events'] + expected_1['events'],
        [expected_2, expected_1]
        )
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
        assert orders[:6] == [
            ['PORT_STOP_BEGIN', 'PORT_STOP_BEGIN', 'PORT_STOP_END',   'PORT_ENTRY',      'PORT_STOP_END',   'PORT_EXIT'],
            ['PORT_STOP_BEGIN', 'PORT_EXIT',       'PORT_STOP_END',   'PORT_STOP_BEGIN', 'PORT_ENTRY',      'PORT_STOP_END'],
            ['PORT_STOP_BEGIN', 'PORT_STOP_BEGIN', 'PORT_EXIT',       'PORT_STOP_END',   'PORT_ENTRY',      'PORT_STOP_END'],
            ['PORT_STOP_BEGIN', 'PORT_STOP_END',   'PORT_STOP_END',   'PORT_ENTRY',      'PORT_STOP_BEGIN', 'PORT_EXIT'],
            ['PORT_STOP_END',   'PORT_STOP_BEGIN', 'PORT_STOP_BEGIN', 'PORT_STOP_END',   'PORT_ENTRY',      'PORT_EXIT'],
            ['PORT_STOP_END',   'PORT_STOP_END',   'PORT_STOP_BEGIN', 'PORT_ENTRY',      'PORT_EXIT',       'PORT_STOP_BEGIN']
        ]


    def test_creation(self):
        target = CreatePortVisits(max_interseg_dist_nm=60.0)
        for events, expected in self.generate_test_cases():
            result = target.create_port_visits(((None, None), events))
            visits = [visit_to_msg(x) for x in result]
            print(visits)
            assert visits == expected
