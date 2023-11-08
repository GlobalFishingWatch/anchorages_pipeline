from pipe_anchorages.transforms.voyages_create import build_voyage, create_voyage
import datetime as dt
import pytz

parse = lambda d:dt.datetime.strptime(d,'%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=pytz.UTC)

def test_build_voyages():
    # builds the voyage in base of a previous visit and after visit
    visits = [
        {
            "visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "ssvid": "366773090",
            "start_timestamp": parse("2012-01-01 00:56:17.000000"),
            "start_lat": "47.529396300347706",
            "start_lon": "-122.49201518365493",
            "start_anchorage_id": "549047a9",
            "end_timestamp": parse("2012-01-01 02:06:46.000000"),
            "end_lat": "47.529396300347706",
            "end_lon": "-122.49201518365493",
            "duration_hrs": "1.1747222222222222",
            "end_anchorage_id": "549047a9",
            "confidence": "4",
            "events": []
        }, {
            "visit_id": "23dfd7e9ddfa3506efc41435b2fcaa45",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "ssvid": "366773090",
            "start_timestamp": parse("2012-01-01 02:33:30.000000"),
            "start_lat": "47.529396300347706",
            "start_lon": "-122.49201518365493",
            "start_anchorage_id": "549047a9",
            "end_timestamp": parse("2012-01-01 03:45:19.000000"),
            "end_lat": "47.529396300347706",
            "end_lon": "-122.49201518365493",
            "duration_hrs": "1.1969444444444444",
            "end_anchorage_id": "549047a9",
            "confidence": "4",
            "events": []
        }
    ]

    assert build_voyage(2, visits[0], visits[1]) == {
        'trip_confidence': 2,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': parse("2012-01-01 02:06:46.000000"),
        'trip_end': parse("2012-01-01 02:33:30.000000"),
        'trip_start_anchorage_id': "549047a9",
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_end_visit_id': "23dfd7e9ddfa3506efc41435b2fcaa45",
        'trip_start_confidence': "4",
        'trip_end_confidence': "4",
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd-01349704def0"
    }

def test_create_voyage_empty_visits():
    # From empty visits return empty voyages
    assert list(create_voyage([None])) == []

def test_create_voyage_visit_with_c4():
    # Creates voyages with a visit of confidence 4, should return init voyages in all trip_confidence
    visits = [{
        "visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
        "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        "ssvid": "366773090",
        "start_timestamp": parse("2012-01-01 00:56:17.000000"),
        "start_lat": "47.529396300347706",
        "start_lon": "-122.49201518365493",
        "start_anchorage_id": "549047a9",
        "end_timestamp": parse("2012-01-01 02:06:46.000000"),
        "end_lat": "47.529396300347706",
        "end_lon": "-122.49201518365493",
        "duration_hrs": "1.1747222222222222",
        "end_anchorage_id": "549047a9",
        "confidence": 4,
        "events": []
    }]

    assert list(create_voyage(visits)) == [{
        'trip_confidence': 2,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': None,
        'trip_end': parse("2012-01-01 00:56:17.000000"),
        'trip_start_anchorage_id': None,
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': None,
        'trip_end_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_start_confidence': None,
        'trip_end_confidence': 4,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd"
    },{
        'trip_confidence': 3,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': None,
        'trip_end': parse("2012-01-01 00:56:17.000000"),
        'trip_start_anchorage_id': None,
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': None,
        'trip_end_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_start_confidence': None,
        'trip_end_confidence': 4,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd"
    },{
        'trip_confidence': 4,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': None,
        'trip_end': parse("2012-01-01 00:56:17.000000"),
        'trip_start_anchorage_id': None,
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': None,
        'trip_end_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_start_confidence': None,
        'trip_end_confidence': 4,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd"
    }]

def test_create_voyage_visit_with_c3():
    # Creates voyages with a visit of confidence 3, should return init voyages in trip_confidence 2,3
    visits = [{
        "visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
        "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        "ssvid": "366773090",
        "start_timestamp": parse("2012-01-01 00:56:17.000000"),
        "start_lat": "47.529396300347706",
        "start_lon": "-122.49201518365493",
        "start_anchorage_id": "549047a9",
        "end_timestamp": parse("2012-01-01 02:06:46.000000"),
        "end_lat": "47.529396300347706",
        "end_lon": "-122.49201518365493",
        "duration_hrs": "1.1747222222222222",
        "end_anchorage_id": "549047a9",
        "confidence": 3,
        "events": []
    }]

    assert list(create_voyage(visits)) == [{
        'trip_confidence': 2,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': None,
        'trip_end': parse("2012-01-01 00:56:17.000000"),
        'trip_start_anchorage_id': None,
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': None,
        'trip_end_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_start_confidence': None,
        'trip_end_confidence': 3,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd"
    },{
        'trip_confidence': 3,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': None,
        'trip_end': parse("2012-01-01 00:56:17.000000"),
        'trip_start_anchorage_id': None,
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': None,
        'trip_end_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_start_confidence': None,
        'trip_end_confidence': 3,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd"
    }]

def test_create_voyage_visit_with_c2():
    # Creates voyages with a visit of confidence 2, should return init voyages in trip_confidence 2
    visits = [{
        "visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
        "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        "ssvid": "366773090",
        "start_timestamp": parse("2012-01-01 00:56:17.000000"),
        "start_lat": "47.529396300347706",
        "start_lon": "-122.49201518365493",
        "start_anchorage_id": "549047a9",
        "end_timestamp": parse("2012-01-01 02:06:46.000000"),
        "end_lat": "47.529396300347706",
        "end_lon": "-122.49201518365493",
        "duration_hrs": "1.1747222222222222",
        "end_anchorage_id": "549047a9",
        "confidence": 2,
        "events": []
    }]

    assert list(create_voyage(visits)) == [{
        'trip_confidence': 2,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': None,
        'trip_end': parse("2012-01-01 00:56:17.000000"),
        'trip_start_anchorage_id': None,
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': None,
        'trip_end_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_start_confidence': None,
        'trip_end_confidence': 2,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd"
    }]

def test_create_voyage_2visits_with_c2():
    # Creates voyages with 2 visits of confidence 2, should return init and middle voyages in trip_confidence 2
    visits = [{
        "visit_id": "5491c9672004f4fb48023b25f27e83bf",
        "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        "ssvid": "366773090",
        "start_timestamp": parse("2012-01-01 00:22:47.000000"),
        "start_lat": "47.529396300347706",
        "start_lon": "-122.49201518365493",
        "start_anchorage_id": "549047a9",
        "end_timestamp": parse("2012-01-01 00:23:47.000000"),
        "end_lat": "47.529396300347706",
        "end_lon": "-122.49201518365493",
        "duration_hrs": "0.0",
        "end_anchorage_id": "549047a9",
        "confidence": 2,
        "events": []
    }, {
        "visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
        "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        "ssvid": "366773090",
        "start_timestamp": parse("2012-01-01 00:56:17.000000"),
        "start_lat": "47.529396300347706",
        "start_lon": "-122.49201518365493",
        "start_anchorage_id": "549047a9",
        "end_timestamp": parse("2012-01-01 02:06:46.000000"),
        "end_lat": "47.529396300347706",
        "end_lon": "-122.49201518365493",
        "duration_hrs": "1.1747222222222222",
        "end_anchorage_id": "549047a9",
        "confidence": 2,
        "events": []
    }]

    assert list(create_voyage(visits)) == [{
        'trip_confidence': 2,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': None,
        'trip_end': parse("2012-01-01 00:22:47.000000"),
        'trip_start_anchorage_id': None,
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': None,
        'trip_end_visit_id': "5491c9672004f4fb48023b25f27e83bf",
        'trip_start_confidence': None,
        'trip_end_confidence': 2,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd"
    },{
        'trip_confidence': 2,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': parse("2012-01-01 00:23:47.000000"),
        'trip_end': parse("2012-01-01 00:56:17.000000"),
        'trip_start_anchorage_id': "549047a9",
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': "5491c9672004f4fb48023b25f27e83bf",
        'trip_end_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_start_confidence': 2,
        'trip_end_confidence': 2,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd-013496a69638"
    }]


def test_create_voyage_visit_with_c2_open():
    # Creates voyages with visits and none at end of confidence 2, should return init and end voyages in trip_confidence 2
    visits = [{
        "visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
        "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        "ssvid": "366773090",
        "start_timestamp": parse("2012-01-01 00:56:17.000000"),
        "start_lat": "47.529396300347706",
        "start_lon": "-122.49201518365493",
        "start_anchorage_id": "549047a9",
        "end_timestamp": parse("2012-01-01 02:06:46.000000"),
        "end_lat": "47.529396300347706",
        "end_lon": "-122.49201518365493",
        "duration_hrs": "1.1747222222222222",
        "end_anchorage_id": "549047a9",
        "confidence": 2,
        "events": []
    }, None] # Notice the addition of None

    assert list(create_voyage(visits)) == [{
        'trip_confidence': 2,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': None,
        'trip_end': parse("2012-01-01 00:56:17.000000"),
        'trip_start_anchorage_id': None,
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': None,
        'trip_end_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_start_confidence': None,
        'trip_end_confidence': 2,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd"
    },{
        'trip_confidence': 2,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': parse("2012-01-01 02:06:46.000000"),
        'trip_end': None,
        'trip_start_anchorage_id': "549047a9",
        'trip_end_anchorage_id': None,
        'trip_start_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_end_visit_id': None,
        'trip_start_confidence': 2,
        'trip_end_confidence': None,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd-01349704def0"
    }]

def test_create_voyage_2visits_with_c34():
    # Creates voyages with visits of confidence 3,4, should return voyages in trip_confidence 2(init,middle),3(init,middle),4(init)
    visits = [{
        "visit_id": "5491c9672004f4fb48023b25f27e83bf",
        "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        "ssvid": "366773090",
        "start_timestamp": parse("2012-01-01 00:22:47.000000"),
        "start_lat": "47.529396300347706",
        "start_lon": "-122.49201518365493",
        "start_anchorage_id": "549047a9",
        "end_timestamp": parse("2012-01-01 00:22:47.000000"),
        "end_lat": "47.529396300347706",
        "end_lon": "-122.49201518365493",
        "duration_hrs": "0.0",
        "end_anchorage_id": "549047a9",
        "confidence": 3,
        "events": []
    }, {
        "visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
        "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        "ssvid": "366773090",
        "start_timestamp": parse("2012-01-01 00:56:17.000000"),
        "start_lat": "47.529396300347706",
        "start_lon": "-122.49201518365493",
        "start_anchorage_id": "549047a9",
        "end_timestamp": parse("2012-01-01 02:06:46.000000"),
        "end_lat": "47.529396300347706",
        "end_lon": "-122.49201518365493",
        "duration_hrs": "1.1747222222222222",
        "end_anchorage_id": "549047a9",
        "confidence": 4,
        "events": []
    }]

    assert list(create_voyage(visits)) == [{
        'trip_confidence': 2,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': None,
        'trip_end': parse("2012-01-01 00:22:47.000000"),
        'trip_start_anchorage_id': None,
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': None,
        'trip_end_visit_id': "5491c9672004f4fb48023b25f27e83bf",
        'trip_start_confidence': None,
        'trip_end_confidence': 3,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd"
    },{
        'trip_confidence': 3,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': None,
        'trip_end': parse("2012-01-01 00:22:47.000000"),
        'trip_start_anchorage_id': None,
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': None,
        'trip_end_visit_id': "5491c9672004f4fb48023b25f27e83bf",
        'trip_start_confidence': None,
        'trip_end_confidence': 3,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd"
    },{
        'trip_confidence': 2,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': parse("2012-01-01 00:22:47.000000"),
        'trip_end': parse("2012-01-01 00:56:17.000000"),
        'trip_start_anchorage_id': "549047a9",
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': "5491c9672004f4fb48023b25f27e83bf",
        'trip_end_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_start_confidence': 3,
        'trip_end_confidence': 4,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd-013496a5abd8"
    },{
        'trip_confidence': 3,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': parse("2012-01-01 00:22:47.000000"),
        'trip_end': parse("2012-01-01 00:56:17.000000"),
        'trip_start_anchorage_id': "549047a9",
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': "5491c9672004f4fb48023b25f27e83bf",
        'trip_end_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_start_confidence': 3,
        'trip_end_confidence': 4,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd-013496a5abd8"
    },{
        'trip_confidence': 4,
        'ssvid': "366773090",
        'vessel_id': '82db144c2-2d0c-5c16-6f47-2f384fc21efd',
        'trip_start': None,
        'trip_end': parse("2012-01-01 00:56:17.000000"),
        'trip_start_anchorage_id': None,
        'trip_end_anchorage_id': "549047a9",
        'trip_start_visit_id': None,
        'trip_end_visit_id': "6f817d7c757b0299fd209cb0a40ab3ab",
        'trip_start_confidence': None,
        'trip_end_confidence': 4,
        'trip_id': "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd"
    }]
