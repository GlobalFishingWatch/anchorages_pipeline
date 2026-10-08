from collections import OrderedDict
import datetime
import numpy as np
import pytest

from pipe_anchorages.core.visit_event import VisitEvent
from pipe_anchorages.core.namedtuples import _datetime_to_s
from pipe_anchorages.pipelines.port_visits.transforms.create_port_visits import CreatePortVisits


expected_1 = OrderedDict(
    [
        ("visit_id", "79028318cc7d067946432ff85274dc51"),
        ("ssvid", "None"),
        ("vessel_id", "None"),
        ("start_timestamp", 1476426402.0),
        ("start_lat", 29.9667462525),
        ("start_lon", 122.4396281067),
        ("start_anchorage_id", "345328af"),
        ("end_timestamp", 1476495140.0),
        ("end_lat", 29.9667462525),
        ("end_lon", 122.4396281067),
        ("end_anchorage_id", "345328af"),
        ("duration_hrs", 19.093888888888888),
        (
            "events",
            [
                OrderedDict(
                    [
                        ("anchorage_id", "345328af"),
                        ("lat", 29.9667462525),
                        ("lon", 122.4396281067),
                        ("vessel_lat", 29.9806137085),
                        ("vessel_lon", 122.4564437866),
                        ("ssvid", "None"),
                        ("seg_id", 412424227),
                        ("vessel_id", "None"),
                        ("timestamp", 1476426402.0),
                        ("event_type", "PORT_ENTRY"),
                        ("last_timestamp", 1476426302.0),
                    ]
                ),
                OrderedDict(
                    [
                        ("anchorage_id", "3452d969"),
                        ("lat", 29.9407869352),
                        ("lon", 122.27906699),
                        ("vessel_lat", 29.9408073425),
                        ("vessel_lon", 122.2787628174),
                        ("ssvid", "None"),
                        ("seg_id", 412424227),
                        ("vessel_id", "None"),
                        ("timestamp", 1476430743.0),
                        ("event_type", "PORT_STOP_BEGIN"),
                        ("last_timestamp", 1476430643.0),
                    ]
                ),
                OrderedDict(
                    [
                        ("anchorage_id", "3452d95d"),
                        ("lat", 29.9399344611),
                        ("lon", 122.2747471358),
                        ("vessel_lat", 29.9394741058),
                        ("vessel_lon", 122.2749481201),
                        ("ssvid", "None"),
                        ("seg_id", 412424227),
                        ("vessel_id", "None"),
                        ("timestamp", 1476436086.0),
                        ("event_type", "PORT_STOP_END"),
                        ("last_timestamp", 1476436006.0),
                    ]
                ),
                OrderedDict(
                    [
                        ("anchorage_id", "3452d95f"),
                        ("lat", 29.941832869),
                        ("lon", 122.2689838563),
                        ("vessel_lat", 29.9417858124),
                        ("vessel_lon", 122.2697219849),
                        ("ssvid", "None"),
                        ("seg_id", 412424227),
                        ("vessel_id", "None"),
                        ("timestamp", 1476488638.0),
                        ("event_type", "PORT_STOP_BEGIN"),
                        ("last_timestamp", 1476436086.0),
                    ]
                ),
                OrderedDict(
                    [
                        ("anchorage_id", "3452d95d"),
                        ("lat", 29.9399344611),
                        ("lon", 122.2747471358),
                        ("vessel_lat", 29.9404067993),
                        ("vessel_lon", 122.2765045166),
                        ("ssvid", "None"),
                        ("seg_id", 412424227),
                        ("vessel_id", "None"),
                        ("timestamp", 1476489542.0),
                        ("event_type", "PORT_STOP_END"),
                        ("last_timestamp", 1476436086.0),
                    ]
                ),
                OrderedDict(
                    [
                        ("anchorage_id", "345328af"),
                        ("lat", 29.9667462525),
                        ("lon", 122.4396281067),
                        ("vessel_lat", 30.0182361603),
                        ("vessel_lon", 122.475944519),
                        ("ssvid", "None"),
                        ("seg_id", 412424227),
                        ("vessel_id", "None"),
                        ("timestamp", 1476495140.0),
                        ("event_type", "PORT_EXIT"),
                        ("last_timestamp", 1476436086.0),
                    ]
                ),
            ],
        ),
        ("confidence", 4),
    ]
)


expected_2 = OrderedDict(
    [
        ("visit_id", "781d4f1a61686b40bb44ffaf5cc6a48a"),
        ("ssvid", "None"),
        ("vessel_id", "None"),
        ("start_timestamp", 1475241790.0),
        ("start_lat", 45.7285312075),
        ("start_lon", 47.6336454619),
        ("start_anchorage_id", "41abf04b"),
        ("end_timestamp", 1475290556.0),
        ("end_lat", 46.3122421582),
        ("end_lon", 47.9769367751),
        ("end_anchorage_id", "41a90e45"),
        ("duration_hrs", 13.546111111111111),
        (
            "events",
            [
                OrderedDict(
                    [
                        ("anchorage_id", "41abf04b"),
                        ("lat", 45.7285312075),
                        ("lon", 47.6336454619),
                        ("vessel_lat", 45.7202682495),
                        ("vessel_lon", 47.6396331787),
                        ("ssvid", "None"),
                        ("seg_id", 273386660),
                        ("vessel_id", "None"),
                        ("timestamp", 1475241790.0),
                        ("event_type", "PORT_ENTRY"),
                        ("last_timestamp", 1476426302.0),
                    ]
                ),
                OrderedDict(
                    [
                        ("anchorage_id", "41a90fa9"),
                        ("lat", 46.3280197425),
                        ("lon", 47.9900577312),
                        ("vessel_lat", 46.3271255493),
                        ("vessel_lon", 47.9901542664),
                        ("ssvid", "None"),
                        ("seg_id", 273386660),
                        ("vessel_id", "None"),
                        ("timestamp", 1475256506.0),
                        ("event_type", "PORT_STOP_BEGIN"),
                        ("last_timestamp", 1476426302.0),
                    ]
                ),
                OrderedDict(
                    [
                        ("anchorage_id", "41a90f75"),
                        ("lat", 46.3406197011),
                        ("lon", 48.0045788836),
                        ("vessel_lat", 46.3408584595),
                        ("vessel_lon", 48.0045700073),
                        ("ssvid", "None"),
                        ("seg_id", 273386660),
                        ("vessel_id", "None"),
                        ("timestamp", 1475258350.0),
                        ("event_type", "PORT_STOP_END"),
                        ("last_timestamp", 1476426302.0),
                    ]
                ),
                OrderedDict(
                    [
                        ("anchorage_id", "41a90f75"),
                        ("lat", 46.3406197011),
                        ("lon", 48.0045788836),
                        ("vessel_lat", 46.3400001526),
                        ("vessel_lon", 48.0033340454),
                        ("ssvid", "None"),
                        ("seg_id", 273386660),
                        ("vessel_id", "None"),
                        ("timestamp", 1475260293.0),
                        ("event_type", "PORT_STOP_BEGIN"),
                        ("last_timestamp", 1476426302.0),
                    ]
                ),
                OrderedDict(
                    [
                        ("anchorage_id", "41a90e45"),
                        ("lat", 46.3122421582),
                        ("lon", 47.9769367751),
                        ("vessel_lat", 46.3120269775),
                        ("vessel_lon", 47.9736251831),
                        ("ssvid", "None"),
                        ("seg_id", 273386660),
                        ("vessel_id", "None"),
                        ("timestamp", 1475287217.0),
                        ("event_type", "PORT_STOP_END"),
                        ("last_timestamp", 1476426302.0),
                    ]
                ),
                OrderedDict(
                    [
                        ("anchorage_id", "41a90e45"),
                        ("lat", 46.3122421582),
                        ("lon", 47.9769367751),
                        ("vessel_lat", 46.1234703064),
                        ("vessel_lon", 47.7903404236),
                        ("ssvid", "None"),
                        ("seg_id", 273386660),
                        ("vessel_id", "None"),
                        ("timestamp", 1475290556.0),
                        ("event_type", "PORT_EXIT"),
                        ("last_timestamp", 1476426302.0),
                    ]
                ),
            ],
        ),
        ("confidence", 4),
    ]
)


expected_3 = [
    OrderedDict(
        [
            ("visit_id", "b988334f552895d23ed4085d3fd014a2"),
            ("ssvid", "None"),
            ("vessel_id", "None"),
            ("start_timestamp", 1476426399.0),
            ("start_lat", 29.941832869),
            ("start_lon", 122.2689838563),
            ("start_anchorage_id", "3452d95f"),
            ("end_timestamp", 1476426401.0),
            ("end_lat", 29.9667462525),
            ("end_lon", 122.4396281067),
            ("end_anchorage_id", "345328af"),
            ("duration_hrs", 0.0005555555555555556),
            (
                "events",
                [
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d95f"),
                            ("lat", 29.941832869),
                            ("lon", 122.2689838563),
                            ("vessel_lat", 29.9417858124),
                            ("vessel_lon", 122.2697219849),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476426399.0),
                            ("event_type", "PORT_STOP_BEGIN"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d95d"),
                            ("lat", 29.9399344611),
                            ("lon", 122.2747471358),
                            ("vessel_lat", 29.9404067993),
                            ("vessel_lon", 122.2765045166),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476426400.0),
                            ("event_type", "PORT_STOP_END"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "345328af"),
                            ("lat", 29.9667462525),
                            ("lon", 122.4396281067),
                            ("vessel_lat", 30.0182361603),
                            ("vessel_lon", 122.475944519),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476426401.0),
                            ("event_type", "PORT_EXIT"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                ],
            ),
            ("confidence", 3),
        ]
    ),
    OrderedDict(
        [
            ("visit_id", "79028318cc7d067946432ff85274dc51"),
            ("ssvid", "None"),
            ("vessel_id", "None"),
            ("start_timestamp", 1476426402.0),
            ("start_lat", 29.9667462525),
            ("start_lon", 122.4396281067),
            ("start_anchorage_id", "345328af"),
            ("end_timestamp", 1476495140.0),
            ("end_lat", 29.9667462525),
            ("end_lon", 122.4396281067),
            ("end_anchorage_id", "345328af"),
            ("duration_hrs", 19.093888888888888),
            (
                "events",
                [
                    OrderedDict(
                        [
                            ("anchorage_id", "345328af"),
                            ("lat", 29.9667462525),
                            ("lon", 122.4396281067),
                            ("vessel_lat", 29.9806137085),
                            ("vessel_lon", 122.4564437866),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476426402.0),
                            ("event_type", "PORT_ENTRY"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d969"),
                            ("lat", 29.9407869352),
                            ("lon", 122.27906699),
                            ("vessel_lat", 29.9408073425),
                            ("vessel_lon", 122.2787628174),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476430743.0),
                            ("event_type", "PORT_STOP_BEGIN"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d95d"),
                            ("lat", 29.9399344611),
                            ("lon", 122.2747471358),
                            ("vessel_lat", 29.9394741058),
                            ("vessel_lon", 122.2749481201),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476436086.0),
                            ("event_type", "PORT_STOP_END"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d95f"),
                            ("lat", 29.941832869),
                            ("lon", 122.2689838563),
                            ("vessel_lat", 29.9417858124),
                            ("vessel_lon", 122.2697219849),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476488638.0),
                            ("event_type", "PORT_STOP_BEGIN"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d95d"),
                            ("lat", 29.9399344611),
                            ("lon", 122.2747471358),
                            ("vessel_lat", 29.9404067993),
                            ("vessel_lon", 122.2765045166),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476489542.0),
                            ("event_type", "PORT_STOP_END"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "345328af"),
                            ("lat", 29.9667462525),
                            ("lon", 122.4396281067),
                            ("vessel_lat", 30.0182361603),
                            ("vessel_lon", 122.475944519),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476495140.0),
                            ("event_type", "PORT_EXIT"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                ],
            ),
            ("confidence", 4),
        ]
    ),
]

# Add some out of order stuff to expected_1.
# The stuff before port_entry at events[3] becomes an event with no start.

events_3 = [
    OrderedDict(
        [
            ("anchorage_id", "3452d95f"),
            ("lat", 29.941832869),
            ("lon", 122.2689838563),
            ("vessel_lat", 29.9417858124),
            ("vessel_lon", 122.2697219849),
            ("ssvid", "None"),
            ("seg_id", 412424227),
            ("vessel_id", "None"),
            ("timestamp", 1476426399.0),
            ("last_timestamp", 1476436086.0),
            ("event_type", "PORT_STOP_BEGIN"),
        ]
    ),
    OrderedDict(
        [
            ("anchorage_id", "3452d95d"),
            ("lat", 29.9399344611),
            ("lon", 122.2747471358),
            ("vessel_lat", 29.9404067993),
            ("vessel_lon", 122.2765045166),
            ("ssvid", "None"),
            ("seg_id", 412424227),
            ("vessel_id", "None"),
            ("timestamp", 1476426400.0),
            ("last_timestamp", 1476436086.0),
            ("event_type", "PORT_STOP_END"),
        ]
    ),
    OrderedDict(
        [
            ("anchorage_id", "345328af"),
            ("lat", 29.9667462525),
            ("lon", 122.4396281067),
            ("vessel_lat", 30.0182361603),
            ("vessel_lon", 122.475944519),
            ("ssvid", "None"),
            ("seg_id", 412424227),
            ("vessel_id", "None"),
            ("timestamp", 1476426401.0),
            ("last_timestamp", 1476436086.0),
            ("event_type", "PORT_EXIT"),
        ]
    ),
    OrderedDict(
        [
            ("anchorage_id", "345328af"),
            ("lat", 29.9667462525),
            ("lon", 122.4396281067),
            ("vessel_lat", 29.9806137085),
            ("vessel_lon", 122.4564437866),
            ("ssvid", "None"),
            ("seg_id", 412424227),
            ("vessel_id", "None"),
            ("timestamp", 1476426402.0),
            ("last_timestamp", 1476436086.0),
            ("event_type", "PORT_ENTRY"),
        ]
    ),
    OrderedDict(
        [
            ("anchorage_id", "3452d969"),
            ("lat", 29.9407869352),
            ("lon", 122.27906699),
            ("vessel_lat", 29.9408073425),
            ("vessel_lon", 122.2787628174),
            ("ssvid", "None"),
            ("seg_id", 412424227),
            ("vessel_id", "None"),
            ("timestamp", 1476430743.0),
            ("last_timestamp", 1476436086.0),
            ("event_type", "PORT_STOP_BEGIN"),
        ]
    ),
    OrderedDict(
        [
            ("anchorage_id", "3452d95d"),
            ("lat", 29.9399344611),
            ("lon", 122.2747471358),
            ("vessel_lat", 29.9394741058),
            ("vessel_lon", 122.2749481201),
            ("ssvid", "None"),
            ("seg_id", 412424227),
            ("vessel_id", "None"),
            ("timestamp", 1476436086.0),
            ("last_timestamp", 1476436086.0),
            ("event_type", "PORT_STOP_END"),
        ]
    ),
    OrderedDict(
        [
            ("anchorage_id", "3452d95f"),
            ("lat", 29.941832869),
            ("lon", 122.2689838563),
            ("vessel_lat", 29.9417858124),
            ("vessel_lon", 122.2697219849),
            ("ssvid", "None"),
            ("seg_id", 412424227),
            ("vessel_id", "None"),
            ("timestamp", 1476488638.0),
            ("last_timestamp", 1476436086.0),
            ("event_type", "PORT_STOP_BEGIN"),
        ]
    ),
    OrderedDict(
        [
            ("anchorage_id", "3452d95d"),
            ("lat", 29.9399344611),
            ("lon", 122.2747471358),
            ("vessel_lat", 29.9404067993),
            ("vessel_lon", 122.2765045166),
            ("ssvid", "None"),
            ("seg_id", 412424227),
            ("vessel_id", "None"),
            ("timestamp", 1476489542.0),
            ("last_timestamp", 1476436086.0),
            ("event_type", "PORT_STOP_END"),
        ]
    ),
    OrderedDict(
        [
            ("anchorage_id", "345328af"),
            ("lat", 29.9667462525),
            ("lon", 122.4396281067),
            ("vessel_lat", 30.0182361603),
            ("vessel_lon", 122.475944519),
            ("ssvid", "None"),
            ("seg_id", 412424227),
            ("vessel_id", "None"),
            ("timestamp", 1476495140.0),
            ("last_timestamp", 1476436086.0),
            ("event_type", "PORT_EXIT"),
        ]
    ),
]


# TODO: I think this may be broken because I used random values for last timestamp check XXX
# Actually not obvious why that would matter, don't think create port_events cares, so why.

# OOPs looks like I messed up when updating event 2, go back to original and redo.

events_4 = [
    OrderedDict(
        [
            ("visit_id", "983f3328bed78676306504f0df69e75e"),
            ("ssvid", "None"),
            ("vessel_id", "None"),
            ("start_timestamp", 1476426402.0),
            ("start_lat", 29.9667462525),
            ("start_lon", 122.4396281067),
            ("start_anchorage_id", "345328af"),
            ("end_timestamp", 1476495140.0),
            ("end_lat", 29.9667462525),
            ("end_lon", 122.4396281067),
            ("end_anchorage_id", "345328af"),
            ("duration_hrs", 19.093888888888888),
            (
                "events",
                [
                    OrderedDict(
                        [
                            ("anchorage_id", "345328af"),
                            ("lat", 29.9667462525),
                            ("lon", 122.4396281067),
                            ("vessel_lat", 29.9806137085),
                            ("vessel_lon", 122.4564437866),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476426402.0),
                            ("event_type", "PORT_ENTRY"),
                            ("last_timestamp", 1476426302.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d969"),
                            ("lat", 29.9407869352),
                            ("lon", 122.27906699),
                            ("vessel_lat", 29.9408073425),
                            ("vessel_lon", 122.2787628174),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476430743.0),
                            ("event_type", "PORT_STOP_BEGIN"),
                            ("last_timestamp", 1476430643.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d969"),
                            ("lat", 29.9407869352),
                            ("lon", 122.27906699),
                            ("vessel_lat", 29.9408073425),
                            ("vessel_lon", 122.2787628174),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476430743.0),
                            ("event_type", "PORT_STOP_BEGIN"),
                            ("last_timestamp", 1476430643.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d95d"),
                            ("lat", 29.9399344611),
                            ("lon", 122.2747471358),
                            ("vessel_lat", 29.9394741058),
                            ("vessel_lon", 122.2749481201),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476436086.0),
                            ("event_type", "PORT_STOP_END"),
                            ("last_timestamp", 1476436006.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d95d"),
                            ("lat", 29.9399344611),
                            ("lon", 122.2747471358),
                            ("vessel_lat", 29.9394741058),
                            ("vessel_lon", 122.2749481201),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476436086.0),
                            ("event_type", "PORT_STOP_END"),
                            ("last_timestamp", 1476436006.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d95f"),
                            ("lat", 29.941832869),
                            ("lon", 122.2689838563),
                            ("vessel_lat", 29.9417858124),
                            ("vessel_lon", 122.2697219849),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476488638.0),
                            ("event_type", "PORT_STOP_BEGIN"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d95f"),
                            ("lat", 29.941832869),
                            ("lon", 122.2689838563),
                            ("vessel_lat", 29.9417858124),
                            ("vessel_lon", 122.2697219849),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476488638.0),
                            ("event_type", "PORT_STOP_BEGIN"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d95d"),
                            ("lat", 29.9399344611),
                            ("lon", 122.2747471358),
                            ("vessel_lat", 29.9404067993),
                            ("vessel_lon", 122.2765045166),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476489542.0),
                            ("event_type", "PORT_STOP_END"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "3452d95d"),
                            ("lat", 29.9399344611),
                            ("lon", 122.2747471358),
                            ("vessel_lat", 29.9404067993),
                            ("vessel_lon", 122.2765045166),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476489542.0),
                            ("event_type", "PORT_STOP_END"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                    OrderedDict(
                        [
                            ("anchorage_id", "345328af"),
                            ("lat", 29.9667462525),
                            ("lon", 122.4396281067),
                            ("vessel_lat", 30.0182361603),
                            ("vessel_lon", 122.475944519),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476495140.0),
                            ("event_type", "PORT_EXIT"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    ),
                ],
            ),
        ]
    ),
    OrderedDict(
        [
            ("visit_id", "111ea0ee948cdd7b6eb76d8109c36b72"),
            ("ssvid", "None"),
            ("vessel_id", "None"),
            ("start_timestamp", 1476495140.0),
            ("start_lat", 29.9667462525),
            ("start_lon", 122.4396281067),
            ("start_anchorage_id", "345328af"),
            ("end_timestamp", 1476495140.0),
            ("end_lat", 29.9667462525),
            ("end_lon", 122.4396281067),
            ("end_anchorage_id", "345328af"),
            ("duration_hrs", 0.0),
            (
                "events",
                [
                    OrderedDict(
                        [
                            ("anchorage_id", "345328af"),
                            ("lat", 29.9667462525),
                            ("lon", 122.4396281067),
                            ("vessel_lat", 30.0182361603),
                            ("vessel_lon", 122.475944519),
                            ("ssvid", "None"),
                            ("seg_id", 412424227),
                            ("vessel_id", "None"),
                            ("timestamp", 1476495140.0),
                            ("event_type", "PORT_EXIT"),
                            ("last_timestamp", 1476436086.0),
                        ]
                    )
                ],
            ),
        ]
    ),
]

expected = [
    (expected_1["events"], [expected_1]),
    (expected_2["events"], [expected_2]),
    (events_3, expected_3),
    # Since time for these two don't overlap, we can use these even though
    # vessel ids are different
    (expected_2["events"] + expected_1["events"], [expected_2, expected_1]),
]


def evt_from_dict(x):
    x["timestamp"] = datetime.datetime.fromtimestamp(x["timestamp"], datetime.UTC)
    return VisitEvent(**x)


def rm_vessel_id_of_events(visit):
    def pop_item(eod, key="vessel_id"):
        if key in eod:
            eod.move_to_end(key)
            eod.popitem()

    for e in visit["events"]:
        pop_item(e)
    return visit


def visit_to_msg(visit):
    def event_to_msg(y):
        y = y._asdict()
        y["timestamp"] = _datetime_to_s(y["timestamp"])
        return y

    visit = visit._asdict()
    visit["events"] = [event_to_msg(e) for e in visit["events"]]
    visit["start_timestamp"] = _datetime_to_s(visit["start_timestamp"])
    visit["end_timestamp"] = _datetime_to_s(visit["end_timestamp"])
    return visit


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
        for events, _ in self.generate_test_cases():
            orders.append([x.event_type for x in events])
        assert orders[:6] == [
            [
                "PORT_STOP_BEGIN",
                "PORT_STOP_BEGIN",
                "PORT_STOP_END",
                "PORT_ENTRY",
                "PORT_STOP_END",
                "PORT_EXIT",
            ],
            [
                "PORT_STOP_BEGIN",
                "PORT_EXIT",
                "PORT_STOP_END",
                "PORT_STOP_BEGIN",
                "PORT_ENTRY",
                "PORT_STOP_END",
            ],
            [
                "PORT_STOP_BEGIN",
                "PORT_STOP_BEGIN",
                "PORT_EXIT",
                "PORT_STOP_END",
                "PORT_ENTRY",
                "PORT_STOP_END",
            ],
            [
                "PORT_STOP_BEGIN",
                "PORT_STOP_END",
                "PORT_STOP_END",
                "PORT_ENTRY",
                "PORT_STOP_BEGIN",
                "PORT_EXIT",
            ],
            [
                "PORT_STOP_END",
                "PORT_STOP_BEGIN",
                "PORT_STOP_BEGIN",
                "PORT_STOP_END",
                "PORT_ENTRY",
                "PORT_EXIT",
            ],
            [
                "PORT_STOP_END",
                "PORT_STOP_END",
                "PORT_STOP_BEGIN",
                "PORT_ENTRY",
                "PORT_EXIT",
                "PORT_STOP_BEGIN",
            ],
        ]

    def test_creation(self):
        target = CreatePortVisits(max_interseg_dist_nm=60.0)
        for events, expected in self.generate_test_cases():
            result = target.create_port_visits(((None, None), events))
            visits = [visit_to_msg(x) for x in result]
            assert visits == expected


def make_event(event_type, minutes, seg_id="seg-1", lat=0.0, lon=0.0):
    t0 = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)
    return VisitEvent(
        anchorage_id="a1", lat=lat, lon=lon, vessel_lat=lat, vessel_lon=lon,
        ssvid="111", seg_id=seg_id, vessel_id="vessel-1",
        timestamp=t0 + datetime.timedelta(minutes=minutes),
        event_type=event_type, last_timestamp=None,
    )


def visits_of(events, max_interseg_dist_nm=60.0):
    target = CreatePortVisits(max_interseg_dist_nm=max_interseg_dist_nm)
    return list(target.create_port_visits(("vessel-1", events)))


@pytest.mark.parametrize(
    "event_types, confidence",
    [
        (["PORT_ENTRY", "PORT_STOP_BEGIN", "PORT_EXIT"], 4),
        (["PORT_ENTRY", "PORT_GAP_BEGIN"], 3),
        (["PORT_GAP_END", "PORT_EXIT"], 3),
        (["PORT_STOP_BEGIN", "PORT_STOP_END"], 2),
        (["PORT_ENTRY", "PORT_EXIT"], 1),
    ],
)
def test_compute_confidence(event_types, confidence):
    events = [make_event(t, i) for i, t in enumerate(event_types)]

    assert CreatePortVisits(60.0).compute_confidence(events) == confidence


def test_compute_confidence_rejects_events_of_no_known_type():
    with pytest.raises(ValueError, match="missing expected event types"):
        CreatePortVisits(60.0).compute_confidence([make_event("UNKNOWN", 0)])


def test_a_visit_ends_at_exit_and_the_next_starts_at_entry():
    events = [
        make_event("PORT_ENTRY", 0), make_event("PORT_EXIT", 10),
        make_event("PORT_ENTRY", 20), make_event("PORT_EXIT", 30),
    ]

    visits = visits_of(events)

    assert [[e.event_type for e in v.events] for v in visits] == [
        ["PORT_ENTRY", "PORT_EXIT"], ["PORT_ENTRY", "PORT_EXIT"]
    ]


def test_events_at_the_same_time_are_ordered_entry_stop_exit():
    events = [make_event("PORT_EXIT", 0), make_event("PORT_STOP_BEGIN", 0),
              make_event("PORT_ENTRY", 0)]

    (visit,) = visits_of(events)

    assert [e.event_type for e in visit.events] == ["PORT_ENTRY", "PORT_STOP_BEGIN", "PORT_EXIT"]


def test_a_visit_is_split_between_segments_too_far_apart():
    events = [
        make_event("PORT_STOP_BEGIN", 0, seg_id="seg-1", lat=0.0),
        make_event("PORT_STOP_END", 10, seg_id="seg-2", lat=2.0),  # 120 nm north
    ]

    assert len(visits_of(events, max_interseg_dist_nm=60.0)) == 2
    assert len(visits_of(events, max_interseg_dist_nm=150.0)) == 1


def test_the_same_segment_is_never_split_by_distance():
    events = [
        make_event("PORT_STOP_BEGIN", 0, lat=0.0), make_event("PORT_STOP_END", 10, lat=2.0)
    ]

    assert len(visits_of(events, max_interseg_dist_nm=60.0)) == 1


def test_unknown_event_types_raise():
    # Documents current behavior: create_port_visits means to log and drop unknown event types,
    # but sorting looks them up in TYPE_ORDER first, which raises.
    events = [make_event("PORT_ENTRY", 0), make_event("UNKNOWN", 5), make_event("PORT_EXIT", 10)]

    with pytest.raises(KeyError, match="UNKNOWN"):
        visits_of(events)


def test_visits_keep_only_the_first_and_last_events_beyond_the_maximum():
    n = CreatePortVisits.MAX_EMITTED_EVENTS + 2
    events = [make_event("PORT_STOP_BEGIN", i) for i in range(n)]

    (visit,) = visits_of(events)

    kept = [e.timestamp for e in visit.events]
    assert len(kept) == CreatePortVisits.MAX_EMITTED_EVENTS
    assert kept[0] == events[0].timestamp and kept[-1] == events[-1].timestamp
    assert events[n // 2].timestamp not in kept
