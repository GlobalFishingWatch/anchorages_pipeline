import datetime
from types import SimpleNamespace

import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that

from gfw.common.beam.transforms.bigquery import FakeReadFromBigQuery

from pipe_anchorages.assets import schemas
from pipe_anchorages.pipelines.port_visits.main import (
    PortStateTransitionsQuery,
    query_windows,
    run,
)
from pipe_anchorages.pipelines.port_visits.table_config import PortVisitsTableConfig

# The in-process runner: DirectRunner would pick Prism, which runs as a subprocess and stages an
# sdist of the package in the working directory.
RUNNER = "FnApiRunner"

T0 = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc).timestamp()
EVERY_SECONDS = 10 * 60

PORT = dict(port_s2id="345328af", port_lat=-34.6, port_lon=-58.4)

# (port_dist km, speed knots) of each position: at sea, in port, stopped, in port, at sea.
SEA_PORT_STOP_PORT_SEA = [(10.0, 10.0), (1.0, 5.0), (0.5, 0.0), (1.0, 5.0), (10.0, 10.0)]


def transitions(track, ssvid="111", vessel_id="vessel-1", seg_id="seg-1"):
    """Rows shaped like the query's output, one per (port_dist, speed) in `track`."""
    return [
        dict(
            ssvid=ssvid,
            vessel_id=vessel_id,
            seg_id=seg_id,
            lat=-34.6 + port_dist / 100,
            lon=-58.4,
            speed=speed,
            is_possible_gap_end=False,
            port_dist=port_dist,
            timestamp=T0 + i * EVERY_SECONDS,
            **PORT,
        )
        for i, (port_dist, speed) in enumerate(track)
    ]


class AssertWritten(beam.PTransform):
    """Stands in for WriteToBigQuery: checks the rows to be written, inside the pipeline."""

    def __init__(self, check, **kwargs):
        super().__init__()
        self._check = check

    def expand(self, pcoll):
        assert_that(pcoll, self._check)
        return pcoll


def run_pipeline(rows, check, reads=None, writes=None, bq_client_factory=None, **overrides):
    """Runs the pipeline on `rows` (shaped like the query's output) and checks what it writes."""

    def read_factory(**kwargs):
        if reads is not None:
            reads.append(kwargs)
        return FakeReadFromBigQuery(elements=rows, **kwargs)

    def write_factory(**kwargs):
        if writes is not None:
            writes.append(kwargs)
        return AssertWritten(check, **kwargs)

    config = dict(
        bq_in_port_state_transitions="project.dataset.port_state_transitions",
        bq_in_segment_info="project.dataset.segment_info",
        bq_out_port_visits="project.dataset.port_visits",
        start_date="2024-01-01",
        end_date="2024-01-07",
        labels={"environment": "development", "stage": "anchorages"},
        mock_bq_clients=True,
        unknown_unparsed_args=[],
        unknown_parsed_args={"project": "test-project"},
    )
    config.update(overrides)

    return run(
        SimpleNamespace(**config),
        read_from_bigquery_factory=read_factory,
        write_to_bigquery_factory=write_factory,
        bq_client_factory=bq_client_factory,
        runner=RUNNER,
    )


def check_one_full_visit(rows):
    assert len(rows) == 1, rows
    (row,) = rows
    assert row["ssvid"] == "111"
    assert row["vessel_id"] == "vessel-1"
    assert row["confidence"] == 4
    assert row["start_timestamp"] == T0 + EVERY_SECONDS
    assert row["end_timestamp"] == T0 + 4 * EVERY_SECONDS
    assert row["duration_hrs"] == pytest.approx(3 * EVERY_SECONDS / 3600)
    assert row["start_anchorage_id"] == row["end_anchorage_id"] == "345328af"
    assert [e["event_type"] for e in row["events"]] == [
        "PORT_ENTRY",
        "PORT_STOP_BEGIN",
        "PORT_STOP_END",
        "PORT_EXIT",
    ]
    (schema_events,) = [f for f in schemas.get_schema("port_visits.json") if f["name"] == "events"]
    assert set(row) == {f["name"] for f in schemas.get_schema("port_visits.json")}
    assert set(row["events"][0]) == {f["name"] for f in schema_events["fields"]}


def check_no_rows(rows):
    assert rows == []


def test_run_detects_a_visit_with_entry_stop_and_exit():
    assert run_pipeline(transitions(SEA_PORT_STOP_PORT_SEA), check_one_full_visit) == 0


def test_run_detects_no_visit_for_a_vessel_that_stays_at_sea():
    assert run_pipeline(transitions([(10.0, 10.0)] * 5), check_no_rows) == 0


def test_run_reads_the_date_range_and_writes_the_output_table():
    reads, writes = [], []

    run_pipeline([], check_no_rows, reads=reads, writes=writes, bad_segs="project.dataset.bad")

    (read,) = reads
    assert "project.dataset.port_state_transitions" in read["query"]
    assert "project.dataset.segment_info" in read["query"]
    assert "DATE(timestamp) >= '2024-01-01'" in read["query"]
    assert "DATE(timestamp) < '2024-01-07'" in read["query"]
    assert "seg_id NOT IN (SELECT seg_id FROM project.dataset.bad)" in read["query"]
    assert read["bigquery_job_labels"] == {"environment": "development", "stage": "anchorages"}
    (write,) = writes
    assert write["table"] == "project.dataset.port_visits"
    assert write["write_disposition"] == beam.io.BigQueryDisposition.WRITE_TRUNCATE
    assert write["create_disposition"] == beam.io.BigQueryDisposition.CREATE_NEVER
    assert [f["name"] for f in write["schema"]["fields"]] == [
        f["name"] for f in schemas.get_schema("port_visits.json")
    ]


def test_run_reads_long_date_ranges_in_several_queries():
    reads = []

    run_pipeline([], check_no_rows, reads=reads, start_date="2020-01-01", end_date="2024-01-02")

    assert len(reads) == 2
    assert "< '2022-09-28'" in reads[0]["query"]
    assert ">= '2022-09-28'" in reads[1]["query"]


def test_run_describes_the_output_table_after_writing_it(bq_clients, bq_client_factory):
    run_pipeline([], check_no_rows, bq_client_factory=bq_client_factory)

    (client,) = bq_clients
    (updated, fields), _ = client.update_table.call_args
    assert fields == ["description", "labels"]
    assert "PORT VISITS" in updated.description
    assert updated.labels == {"environment": "development", "stage": "anchorages"}


def test_output_table_layout():
    table_config = PortVisitsTableConfig(table_id="project.dataset.port_visits")

    params = table_config.to_bigquery_params()

    assert (params["partition_type"], params["partition_field"]) == ("MONTH", "end_timestamp")
    assert params["clustering_fields"] == ("end_timestamp",)


def test_query_windows_split_the_range_into_consecutive_windows():
    windows = list(query_windows(datetime.date(2020, 1, 1), datetime.date(2024, 1, 2)))

    assert windows == [
        (datetime.date(2020, 1, 1), datetime.date(2022, 9, 28)),
        (datetime.date(2022, 9, 28), datetime.date(2024, 1, 2)),
    ]


def test_query_windows_of_an_empty_range():
    assert list(query_windows(datetime.date(2020, 1, 1), datetime.date(2020, 1, 1))) == []


def test_query_renders_without_bad_segs():
    query = PortStateTransitionsQuery(
        source_port_state_transitions="TRANSITIONS",
        source_segment_info="SEGMENT_INFO",
        start_date="2016-01-01",
        end_date="2016-01-02",
    )
    expected = """
SELECT
    vids.ssvid,
    vids.vessel_id,
    vids.seg_id,
    records.* EXCEPT (timestamp, identifier),
    CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp
FROM
    `TRANSITIONS` records
JOIN
    `SEGMENT_INFO` vids
ON
    records.identifier = vids.seg_id
WHERE
    DATE(timestamp) >= '2016-01-01'
    AND DATE(timestamp) < '2016-01-02'
"""
    assert query.render() == expected.strip("\n")
