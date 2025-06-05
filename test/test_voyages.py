from pipe_anchorages.voyages.transforms.create import build_voyage, create_voyage
from pipe_anchorages.voyages.transforms.read_source import SOURCE_QUERY_TEMPLATE
import datetime as dt
import pytz


def parse(d): return dt.datetime.strptime(d, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=pytz.UTC)
def parse_date(d): return dt.datetime.strptime(d, "%Y-%m-%d")


def test_query():
    assert (
        SOURCE_QUERY_TEMPLATE.format(
            source_table="source",
            start=parse_date("2012-01-01"),
        )
        == """
    SELECT
        ssvid,
        vessel_id,
        visit_id,
        start_anchorage_id,
        start_timestamp,
        end_anchorage_id,
        end_timestamp,
        confidence
    FROM
      `source`
    WHERE
        date(end_timestamp) >= "2012-01-01"
        AND confidence >= 2

"""
    )


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
            "events": [],
        },
        {
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
            "events": [],
        },
    ]

    assert build_voyage(2, visits[0], visits[1]) == {
        "trip_confidence": 2,
        "ssvid": "366773090",
        "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        "trip_start": parse("2012-01-01 02:06:46.000000"),
        "trip_end": parse("2012-01-01 02:33:30.000000"),
        "trip_start_anchorage_id": "549047a9",
        "trip_end_anchorage_id": "549047a9",
        "trip_start_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
        "trip_end_visit_id": "23dfd7e9ddfa3506efc41435b2fcaa45",
        "trip_start_confidence": "4",
        "trip_end_confidence": "4",
        "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd-01349704def0",
    }


def test_create_voyage_empty_visits():
    # From empty visits return empty voyages
    assert list(create_voyage([None])) == []


def test_create_voyage_visit_with_c4():
    # Creates voyages with a visit of confidence 4, should return init voyages
    # in all trip_confidence
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
            "confidence": 4,
            "events": [],
        }
    ]

    assert list(create_voyage(visits)) == [
        {
            "trip_confidence": 2,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": None,
            "trip_end": parse("2012-01-01 00:56:17.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "trip_start_confidence": None,
            "trip_end_confidence": 4,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        },
        {
            "trip_confidence": 3,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": None,
            "trip_end": parse("2012-01-01 00:56:17.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "trip_start_confidence": None,
            "trip_end_confidence": 4,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        },
        {
            "trip_confidence": 4,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": None,
            "trip_end": parse("2012-01-01 00:56:17.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "trip_start_confidence": None,
            "trip_end_confidence": 4,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        },
    ]


def test_create_voyage_visit_with_c3():
    # Creates voyages with a visit of confidence 3, should return init voyages
    # in trip_confidence 2,3
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
            "confidence": 3,
            "events": [],
        }
    ]

    assert list(create_voyage(visits)) == [
        {
            "trip_confidence": 2,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": None,
            "trip_end": parse("2012-01-01 00:56:17.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "trip_start_confidence": None,
            "trip_end_confidence": 3,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        },
        {
            "trip_confidence": 3,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": None,
            "trip_end": parse("2012-01-01 00:56:17.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "trip_start_confidence": None,
            "trip_end_confidence": 3,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        },
    ]


def test_create_voyage_visit_with_c2():
    # Creates voyages with a visit of confidence 2, should return init voyages in trip_confidence 2
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
            "confidence": 2,
            "events": [],
        }
    ]

    assert list(create_voyage(visits)) == [
        {
            "trip_confidence": 2,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": None,
            "trip_end": parse("2012-01-01 00:56:17.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "trip_start_confidence": None,
            "trip_end_confidence": 2,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        }
    ]


def test_create_voyage_2visits_with_c2():
    # Creates voyages with 2 visits of confidence 2, should return init and
    # middle voyages in trip_confidence 2
    visits = [
        {
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
            "events": [],
        },
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
            "confidence": 2,
            "events": [],
        },
    ]

    assert list(create_voyage(visits)) == [
        {
            "trip_confidence": 2,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": None,
            "trip_end": parse("2012-01-01 00:22:47.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "5491c9672004f4fb48023b25f27e83bf",
            "trip_start_confidence": None,
            "trip_end_confidence": 2,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        },
        {
            "trip_confidence": 2,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": parse("2012-01-01 00:23:47.000000"),
            "trip_end": parse("2012-01-01 00:56:17.000000"),
            "trip_start_anchorage_id": "549047a9",
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": "5491c9672004f4fb48023b25f27e83bf",
            "trip_end_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "trip_start_confidence": 2,
            "trip_end_confidence": 2,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd-013496a69638",
        },
    ]


def test_create_voyage_visit_with_c2_open():
    # Creates voyages with visits and none at end of confidence 2, should
    # return init and end voyages in trip_confidence 2
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
            "confidence": 2,
            "events": [],
        },
        None,
    ]  # Notice the addition of None

    assert list(create_voyage(visits)) == [
        {
            "trip_confidence": 2,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": None,
            "trip_end": parse("2012-01-01 00:56:17.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "trip_start_confidence": None,
            "trip_end_confidence": 2,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        },
        {
            "trip_confidence": 2,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": parse("2012-01-01 02:06:46.000000"),
            "trip_end": None,
            "trip_start_anchorage_id": "549047a9",
            "trip_end_anchorage_id": None,
            "trip_start_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "trip_end_visit_id": None,
            "trip_start_confidence": 2,
            "trip_end_confidence": None,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd-01349704def0",
        },
    ]


def test_create_voyage_2visits_with_c34():
    # Creates voyages with visits of confidence 3,4, should return voyages in
    # trip_confidence 2(init,middle),3(init,middle),4(init)
    visits = [
        {
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
            "events": [],
        },
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
            "confidence": 4,
            "events": [],
        },
    ]

    assert list(create_voyage(visits)) == [
        {
            "trip_confidence": 2,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": None,
            "trip_end": parse("2012-01-01 00:22:47.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "5491c9672004f4fb48023b25f27e83bf",
            "trip_start_confidence": None,
            "trip_end_confidence": 3,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        },
        {
            "trip_confidence": 3,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": None,
            "trip_end": parse("2012-01-01 00:22:47.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "5491c9672004f4fb48023b25f27e83bf",
            "trip_start_confidence": None,
            "trip_end_confidence": 3,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        },
        {
            "trip_confidence": 2,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": parse("2012-01-01 00:22:47.000000"),
            "trip_end": parse("2012-01-01 00:56:17.000000"),
            "trip_start_anchorage_id": "549047a9",
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": "5491c9672004f4fb48023b25f27e83bf",
            "trip_end_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "trip_start_confidence": 3,
            "trip_end_confidence": 4,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd-013496a5abd8",
        },
        {
            "trip_confidence": 3,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": parse("2012-01-01 00:22:47.000000"),
            "trip_end": parse("2012-01-01 00:56:17.000000"),
            "trip_start_anchorage_id": "549047a9",
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": "5491c9672004f4fb48023b25f27e83bf",
            "trip_end_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "trip_start_confidence": 3,
            "trip_end_confidence": 4,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd-013496a5abd8",
        },
        {
            "trip_confidence": 4,
            "ssvid": "366773090",
            "vessel_id": "82db144c2-2d0c-5c16-6f47-2f384fc21efd",
            "trip_start": None,
            "trip_end": parse("2012-01-01 00:56:17.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "549047a9",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "6f817d7c757b0299fd209cb0a40ab3ab",
            "trip_start_confidence": None,
            "trip_end_confidence": 4,
            "trip_id": "366773090-82db144c2-2d0c-5c16-6f47-2f384fc21efd",
        },
    ]


def test_create_voyage_should_have_duration_btw2visits():
    visits = [
        {
            "visit_id": "ff7e721df08d8b4301e9675613b42f06",
            "vessel_id": "03d263209-98dc-ad84-2f7a-2eba4539aa14",
            "ssvid": "244670089",
            "start_timestamp": parse("2012-12-10 05:18:50.000000"),
            "start_lat": "51.444202554780254",
            "start_lon": "4.0118910160172163",
            "start_anchorage_id": "47c47b4d",
            "end_timestamp": parse("2012-12-31 10:48:13.000000"),
            "end_lat": "51.454323394619728",
            "end_lon": "6.7865645557114567",
            "duration_hrs": "509.48972222222221",
            "end_anchorage_id": "47b8bf9b",
            "confidence": 3,
            "events": [],
        },
        {
            "visit_id": "77955dc2f46d7d4dd2dc7c754dbfede2",
            "vessel_id": "03d263209-98dc-ad84-2f7a-2eba4539aa14",
            "ssvid": "244670089",
            "start_timestamp": parse("2012-12-31 10:48:13.000000"),
            "start_lat": "51.903080156028977",
            "start_lon": "4.18745721026542",
            "start_anchorage_id": "47c44d9f",
            "end_timestamp": parse("2013-01-03 12:11:21.000000"),
            "end_lat": "51.32896919721874",
            "end_lon": "6.6908749026664012",
            "duration_hrs": "73.385555555555555",
            "end_anchorage_id": "47b8b9e1",
            "confidence": 3,
            "events": [],
        },
        {
            "visit_id": "39d2e5e0b78eb65d3cf7f7a1ef7212c6",
            "vessel_id": "03d263209-98dc-ad84-2f7a-2eba4539aa14",
            "ssvid": "244670089",
            "start_timestamp": parse("2013-01-03 12:24:45.000000"),
            "start_lat": "51.256890312061159",
            "start_lon": "6.721973551543039",
            "start_anchorage_id": "47b8b669",
            "end_timestamp": parse("2013-01-09 13:12:48.000000"),
            "end_lat": "51.352089617662273",
            "end_lon": "4.2513254967104954",
            "duration_hrs": "144.80083333333334",
            "end_anchorage_id": "47c4749d",
            "confidence": 4,
            "events": [],
        },
    ]
    # NOTICE that the end_timestamp of 1st and the start_timestamp of 2nd are same exact time

    assert list(create_voyage(visits)) == [
        {
            "trip_confidence": 2,
            "ssvid": "244670089",
            "vessel_id": "03d263209-98dc-ad84-2f7a-2eba4539aa14",
            "trip_start": None,
            "trip_end": parse("2012-12-10 05:18:50.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "47c47b4d",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "ff7e721df08d8b4301e9675613b42f06",
            "trip_start_confidence": None,
            "trip_end_confidence": 3,
            "trip_id": "244670089-03d263209-98dc-ad84-2f7a-2eba4539aa14",
        },
        {
            "trip_confidence": 3,
            "ssvid": "244670089",
            "vessel_id": "03d263209-98dc-ad84-2f7a-2eba4539aa14",
            "trip_start": None,
            "trip_end": parse("2012-12-10 05:18:50.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "47c47b4d",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "ff7e721df08d8b4301e9675613b42f06",
            "trip_start_confidence": None,
            "trip_end_confidence": 3,
            "trip_id": "244670089-03d263209-98dc-ad84-2f7a-2eba4539aa14",
        },
        {
            "trip_confidence": 2,
            "ssvid": "244670089",
            "vessel_id": "03d263209-98dc-ad84-2f7a-2eba4539aa14",
            "trip_start": parse("2012-12-31 10:48:13.000000"),
            "trip_end": parse("2013-01-03 12:24:45.000000"),
            "trip_start_anchorage_id": "47b8bf9b",
            "trip_end_anchorage_id": "47b8b669",
            "trip_start_visit_id": "ff7e721df08d8b4301e9675613b42f06",
            "trip_end_visit_id": "39d2e5e0b78eb65d3cf7f7a1ef7212c6",
            "trip_start_confidence": 3,
            "trip_end_confidence": 4,
            "trip_id": "244670089-03d263209-98dc-ad84-2f7a-2eba4539aa14-013bf09371c8",
        },
        {
            "trip_confidence": 3,
            "ssvid": "244670089",
            "vessel_id": "03d263209-98dc-ad84-2f7a-2eba4539aa14",
            "trip_start": parse("2012-12-31 10:48:13.000000"),
            "trip_end": parse("2013-01-03 12:24:45.000000"),
            "trip_start_anchorage_id": "47b8bf9b",
            "trip_end_anchorage_id": "47b8b669",
            "trip_start_visit_id": "ff7e721df08d8b4301e9675613b42f06",
            "trip_end_visit_id": "39d2e5e0b78eb65d3cf7f7a1ef7212c6",
            "trip_start_confidence": 3,
            "trip_end_confidence": 4,
            "trip_id": "244670089-03d263209-98dc-ad84-2f7a-2eba4539aa14-013bf09371c8",
        },
        {
            "trip_confidence": 4,
            "ssvid": "244670089",
            "vessel_id": "03d263209-98dc-ad84-2f7a-2eba4539aa14",
            "trip_start": None,
            "trip_end": parse("2013-01-03 12:24:45.000000"),
            "trip_start_anchorage_id": None,
            "trip_end_anchorage_id": "47b8b669",
            "trip_start_visit_id": None,
            "trip_end_visit_id": "39d2e5e0b78eb65d3cf7f7a1ef7212c6",
            "trip_start_confidence": None,
            "trip_end_confidence": 4,
            "trip_id": "244670089-03d263209-98dc-ad84-2f7a-2eba4539aa14",
        },
    ]
