import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that

from pipe_anchorages.common import LatLon
from pipe_anchorages.core.visit_location_record import VisitLocationRecord
from pipe_anchorages.pipelines.port_state_transitions.transforms.core import (
    FindPortStateTransitions,
    record_to_msg,
)

from .factories import KM_PER_DEGREE_LAT, NAMED_ANCHORAGE, PORT_LAT, PORT_LON, T0, at


def test_record_to_msg_flattens_the_location_and_converts_the_timestamp_to_seconds():
    record = VisitLocationRecord(
        identifier="seg-1",
        timestamp=at(10),
        location=LatLon(-34.5, -58.3),
        speed=5.0,
        is_possible_gap_end=True,
        port_s2id="port-1",
        port_dist=1.0,
        port_lon=PORT_LON,
        port_lat=PORT_LAT,
    )

    assert record_to_msg(record) == dict(
        identifier="seg-1",
        timestamp=T0.timestamp() + 600,
        lat=-34.5,
        lon=-58.3,
        speed=5.0,
        is_possible_gap_end=True,
        port_s2id="port-1",
        port_dist=1.0,
        port_lon=PORT_LON,
        port_lat=PORT_LAT,
    )


def test_find_port_state_transitions_turns_query_rows_into_output_rows(pipeline):
    rows = [
        dict(
            ident="seg-1",
            ssvid="111",
            lat=PORT_LAT + km / KM_PER_DEGREE_LAT,
            lon=PORT_LON,
            speed=speed,
            timestamp=T0.timestamp() + i * 600,
        )
        for i, (km, speed) in enumerate([(10.0, 10.0), (1.0, 5.0), (10.0, 10.0)])
    ]

    def check(output):
        output = sorted(output, key=lambda r: r["timestamp"])
        assert [r["timestamp"] for r in output] == [T0.timestamp() + i * 600 for i in range(3)]
        assert output[1]["port_s2id"] == "port-1"
        assert output[1]["port_dist"] == pytest.approx(1.0, abs=0.1)

    with pipeline as p:
        transform = FindPortStateTransitions(
            anchorage_entry_dist_km=3.0,
            anchorage_exit_dist_km=4.0,
            stopping_speed_knots=0.2,
            starting_speed_knots=0.5,
            min_anchorage_gap_minutes=240.0,
            start_date=T0.date(),
            end_date=at(24 * 60).date(),
        )
        transform.set_side_inputs(p | "Anchorages" >> beam.Create([NAMED_ANCHORAGE]))
        assert_that(p | "Rows" >> beam.Create(rows) | transform, check)
