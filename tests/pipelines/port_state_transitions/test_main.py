import datetime
from types import SimpleNamespace

import apache_beam as beam
from apache_beam.testing.util import assert_that

from gfw.common.beam.transforms.bigquery import FakeReadFromBigQuery

from pipe_anchorages.assets import schemas
from pipe_anchorages.pipelines.port_state_transitions.main import (
    NamedAnchoragesQuery,
    read_ssvid_filter,
    run,
)
from pipe_anchorages.pipelines.port_state_transitions.table_config import (
    PortStateTransitionsTableConfig,
)

# The in-process runner: DirectRunner would pick Prism, which runs as a subprocess and stages an
# sdist of the package in the working directory.
RUNNER = "FnApiRunner"

T0 = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc).timestamp()
EVERY_SECONDS = 10 * 60

PORT_LAT, PORT_LON = -34.6, -58.4
KM_PER_DEGREE_LAT = 111.2

NAMED_ANCHORAGES = [
    dict(anchor_lat=PORT_LAT, anchor_lon=PORT_LON, anchor_id="port-1", label="BUENOS AIRES"),
]

# (km from the port, speed knots) of each position: at sea, in port, stopped, in port, at sea.
SEA_PORT_STOP_PORT_SEA = [(10.0, 10.0), (1.0, 5.0), (0.5, 0.0), (1.0, 5.0), (10.0, 10.0)]
AT_SEA = [(10.0, 10.0)] * 5


def messages(track, seg_id="seg-1"):
    """Rows shaped like the messages query's output, one per (km, speed) in `track`."""
    return [
        dict(
            ident=seg_id,
            lat=PORT_LAT + km / KM_PER_DEGREE_LAT,
            lon=PORT_LON,
            timestamp=T0 + i * EVERY_SECONDS,
            speed=speed,
        )
        for i, (km, speed) in enumerate(track)
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
    """Runs the pipeline on `rows` (shaped like the messages query's output) and checks its output.

    The named anchorages query gets NAMED_ANCHORAGES.
    """

    def read_factory(**kwargs):
        if reads is not None:
            reads.append(kwargs)
        if "anchor_lat" in kwargs["query"]:
            return FakeReadFromBigQuery(elements=NAMED_ANCHORAGES, **kwargs)
        return FakeReadFromBigQuery(elements=rows, **kwargs)

    def write_factory(**kwargs):
        if writes is not None:
            writes.append(kwargs)
        return AssertWritten(check, **kwargs)

    config = dict(
        bq_in_named_anchorages="project.dataset.named_anchorages",
        bq_in_messages="project.dataset.messages",
        bq_out_port_state_transitions="project.dataset.port_state_transitions",
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


def check_all_positions_around_transitions(rows):
    # Every position is next to a transition (enter, stop, start, exit), so all are kept.
    rows = sorted(rows, key=lambda r: r["timestamp"])
    assert [r["timestamp"] for r in rows] == [T0 + i * EVERY_SECONDS for i in range(5)]
    assert {r["identifier"] for r in rows} == {"seg-1"}
    assert [r["port_s2id"] for r in rows[1:4]] == ["port-1"] * 3
    assert [r["is_possible_gap_end"] for r in rows] == [True, False, False, False, False]
    assert set(rows[0]) == {f["name"] for f in schemas.get_schema("port_state_transitions.json")}


def check_only_the_first_and_last_positions(rows):
    # No transitions at sea: only the first and last position of the segment-day are kept.
    rows = sorted(rows, key=lambda r: r["timestamp"])
    assert [r["timestamp"] for r in rows] == [T0, T0 + 4 * EVERY_SECONDS]
    # The port fields hold the nearest anchorage, even when the vessel isn't in port.
    assert [r["port_s2id"] for r in rows] == ["port-1", "port-1"]
    assert [round(r["port_dist"]) for r in rows] == [10, 10]


def check_no_rows(rows):
    assert rows == []


def test_run_keeps_the_positions_around_port_state_transitions():
    run_pipeline(messages(SEA_PORT_STOP_PORT_SEA), check_all_positions_around_transitions)


def test_run_keeps_only_the_day_edges_for_a_vessel_that_stays_at_sea():
    run_pipeline(messages(AT_SEA), check_only_the_first_and_last_positions)


def test_run_reads_the_date_range_and_the_anchorages_and_appends_to_the_output_table():
    reads, writes = [], []

    run_pipeline([], check_no_rows, reads=reads, writes=writes, ssvid_filter="'111', '222'")

    messages_read, anchorages_read = sorted(reads, key=lambda r: "anchor_lat" in r["query"])
    assert "project.dataset.messages" in messages_read["query"]
    assert "seg_id AS ident" in messages_read["query"]
    assert "destination" not in messages_read["query"]
    assert "date(timestamp) >= '2024-01-01'" in messages_read["query"]
    assert "date(timestamp) < '2024-01-07'" in messages_read["query"]
    assert "AND ssvid IN ('111', '222')" in messages_read["query"]
    assert "project.dataset.named_anchorages" in anchorages_read["query"]
    for read in reads:
        assert read["bigquery_job_labels"] == {"environment": "development", "stage": "anchorages"}
    (write,) = writes
    assert write["table"] == "project.dataset.port_state_transitions"
    assert write["write_disposition"] == beam.io.BigQueryDisposition.WRITE_APPEND
    assert write["create_disposition"] == beam.io.BigQueryDisposition.CREATE_NEVER
    assert [f["name"] for f in write["schema"]["fields"]] == [
        f["name"] for f in schemas.get_schema("port_state_transitions.json")
    ]


def test_run_describes_the_output_table_after_writing_it(bq_clients, bq_client_factory):
    run_pipeline([], check_no_rows, bq_client_factory=bq_client_factory)

    (client,) = bq_clients
    (updated, fields), _ = client.update_table.call_args
    assert fields == ["description", "labels"]
    assert "PORT STATE TRANSITIONS" in updated.description
    assert updated.labels == {"environment": "development", "stage": "anchorages"}


def test_output_table_layout():
    table_config = PortStateTransitionsTableConfig(table_id="project.dataset.output")

    params = table_config.to_bigquery_params()

    assert (params["partition_type"], params["partition_field"]) == ("MONTH", "timestamp")
    assert params["clustering_fields"] == ("timestamp", "is_possible_gap_end")


def test_delete_query_clears_the_processed_date_range():
    table_config = PortStateTransitionsTableConfig(table_id="project.dataset.output")

    query = table_config.delete_query(datetime.date(2024, 1, 1), datetime.date(2024, 1, 7))

    assert query == (
        "DELETE FROM `project.dataset.output` "
        "WHERE DATE(timestamp) >= '2024-01-01' AND DATE(timestamp) < '2024-01-07'"
    )


def test_named_anchorages_query_renders():
    query = NamedAnchoragesQuery(source_named_anchorages="ANCHORAGES")

    assert "FROM\n    `ANCHORAGES`" in query.render()


def test_ssvid_filter_is_read_from_a_file_when_prefixed_by_at(tmp_path):
    path = tmp_path / "ssvids.txt"
    path.write_text("'111', '222'")

    assert read_ssvid_filter(f"@{path}") == "'111', '222'"
    assert read_ssvid_filter("'333'") == "'333'"
    assert read_ssvid_filter(None) is None
