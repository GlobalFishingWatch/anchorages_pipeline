from pipe_anchorages.transforms.voyages_create import build_voyage
import datetime as dt
import pytz

def test_build_voyages():
    parse = lambda d:dt.datetime.strptime(d,'%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=pytz.UTC)
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


