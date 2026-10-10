import datetime
from types import SimpleNamespace

import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that

from gfw.common.beam.transforms.bigquery import FakeReadFromBigQuery
from gfw.common.bigquery.helper import BigQueryHelper

from pipe_anchorages.assets import schemas
from pipe_anchorages.pipelines.anchorage_locations.main import run

# Beam's local runner, named explicitly: DirectRunner would fall back to other runners for
# pipelines Prism doesn't support.
RUNNER = "PrismRunner"

T0 = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc).timestamp()
STAY_POSITIONS = 40
EVERY_MINUTES = 10
STAY_HOURS = (STAY_POSITIONS - 1) * EVERY_MINUTES / 60


def stay_then_leave(ssvid, lat, lon):
    """Positions of a vessel stopped at (lat, lon) for STAY_HOURS, then moving ~5 km away.

    A stationary period is only closed (and counted) once the vessel moves away.
    """
    stay = [
        dict(ident=ssvid, lat=lat, lon=lon, timestamp=T0 + i * EVERY_MINUTES * 60,
             destination=None, speed=0.0)
        for i in range(STAY_POSITIONS)
    ]
    leave = dict(ident=ssvid, lat=lat + 0.05, lon=lon,
                 timestamp=T0 + STAY_POSITIONS * EVERY_MINUTES * 60, destination=None, speed=10.0)
    return stay + [leave]


TWO_VESSELS_AT_ONE_ANCHORAGE = (
    stay_then_leave("111", -34.6, -58.4) + stay_then_leave("222", -34.6001, -58.4001)
)


class AssertWritten(beam.PTransform):
    """Stands in for WriteToBigQuery: checks the rows to be written, inside the pipeline."""

    def __init__(self, check, **kwargs):
        super().__init__()
        self._check = check

    def expand(self, pcoll):
        assert_that(pcoll, self._check)
        return pcoll


@pytest.fixture
def fishing_ssvids(tmp_path):
    def write(*ssvids):
        path = tmp_path / "fishing_mmsi.txt"
        path.write_text("".join(f"{s}\n" for s in ssvids) or "999\n")
        return str(path)

    return write


def run_pipeline(
    rows, check, fishing_ssvids_path, reads=None, writes=None, bq_client_factory=None, **overrides
):
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
        bq_in_messages="project.dataset.messages_positions",
        bq_out_anchorage_locations="project.dataset.anchorage_locations",
        start_date="2024-01-01",
        end_date="2024-01-07",
        gcs_in_fishing_ssvids=fishing_ssvids_path,
        min_positions=10,
        min_unique_vessels=2,
        stationary_period_min_duration_minutes=60,
        stationary_period_max_distance_km=0.5,
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
        bq_client_factory=bq_client_factory or BigQueryHelper.get_client_factory(mocked=True),
        runner=RUNNER,
    )


def check_one_anchorage(rows):
    # 111 is a fishing vessel, 222 isn't.
    assert len(rows) == 1, rows
    (row,) = rows
    assert row["total_visits"] == 2
    assert row["unique_stationary_ssvid"] == 2
    assert row["unique_total_ssvid"] == 2
    assert row["stationary_ssvid_days"] == pytest.approx(2 * STAY_HOURS / 24)
    assert row["unique_stationary_fishing_ssvid"] == 1
    assert row["stationary_fishing_ssvid_days"] == pytest.approx(STAY_HOURS / 24)
    assert row["lat"] == pytest.approx(-34.60005)
    assert row["lon"] == pytest.approx(-58.40005)
    assert set(row) == {f["name"] for f in schemas.get_schema("anchorage_locations.json")}


def test_run_finds_the_anchorage_where_vessels_stay(fishing_ssvids, bq_clients, bq_client_factory):
    # The one test that runs the pipeline in Beam: the transforms' own tests cover each case.
    reads, writes = [], []

    run_pipeline(
        TWO_VESSELS_AT_ONE_ANCHORAGE,
        check_one_anchorage,
        fishing_ssvids("111"),
        reads=reads,
        writes=writes,
        bq_client_factory=bq_client_factory,
    )

    (read,) = reads
    assert "project.dataset.messages_positions" in read["query"]
    assert "date(timestamp) >= '2024-01-01'" in read["query"]
    assert "date(timestamp) < '2024-01-07'" in read["query"]
    (write,) = writes
    assert write["table"] == "project.dataset.anchorage_locations"
    assert write["create_disposition"] == beam.io.BigQueryDisposition.CREATE_NEVER
    assert "additional_bq_parameters" not in write
    assert [f["name"] for f in write["schema"]["fields"]] == [
        f["name"] for f in schemas.get_schema("anchorage_locations.json")
    ]
    # The output table is described after writing it.
    (update_client,) = bq_clients
    (updated, fields), _ = update_client.update_table.call_args
    assert fields == ["description", "labels"]
    assert "ANCHORAGE POINTS" in updated.description
    assert updated.labels == {"environment": "development", "stage": "anchorages"}
