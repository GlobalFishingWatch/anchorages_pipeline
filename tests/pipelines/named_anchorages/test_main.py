import pathlib
from types import SimpleNamespace

import apache_beam as beam
from apache_beam.testing.util import assert_that

from gfw.common.beam.transforms.bigquery import FakeReadFromBigQuery

from pipe_anchorages.assets import schemas
from pipe_anchorages.pipelines.named_anchorages.main import AnchorageLocationsQuery, run
from pipe_anchorages.pipelines.named_anchorages.table_config import NamedAnchoragesTableConfig

# Beam's local runner, named explicitly: DirectRunner would fall back to other runners for
# pipelines Prism doesn't support.
RUNNER = "PrismRunner"

# A few rows of the real anchorage_overrides.csv, so the pipeline doesn't push its 60410 through
# Beam: TONGYEONG and REMOVE, which the input has, POINTE NOIRE, which it doesn't, and two OLIB
# rows in the same S2 cell. The pipeline adds every override whose s2id isn't among its input's,
# and drops the one labelled REMOVE.
ANCHORAGE_OVERRIDES = str(pathlib.Path(__file__).parent / "anchorage_overrides.csv")
N_OVERRIDES = 5


def anchorage_location(s2id, lat, lon, top_destination=None):
    """A row shaped like the anchorage locations query's output."""
    return dict(
        s2id=s2id,
        lat=lat,
        lon=lon,
        total_visits=10,
        drift_radius=0.1,
        top_destination=top_destination,
        unique_stationary_ssvid=3,
        unique_stationary_fishing_ssvid=1,
        unique_active_ssvid=5,
        unique_total_ssvid=6,
        active_ssvid_days=2.0,
        stationary_ssvid_days=1.5,
        stationary_fishing_ssvid_days=0.5,
    )


ANCHORAGE_LOCATIONS = [
    # Within 4 km of Thunder Bay, a WPI port, but not within 1 km of it: no sublabel.
    anchorage_location("thunder-bay", 48.4, -89.25),
    # Far from any known place: named after the destination its vessels report.
    anchorage_location("pacific", -30.0, -120.0, top_destination="Somewhere"),
    # Far from any known place, and no destination: named after its s2id.
    anchorage_location("pacific-2", -30.5, -120.5),
    # In China's EEZ, far from any known place: named after its s2id.
    anchorage_location("east-china-sea", 30.0, 124.0),
    # An anchorage of the overrides: keeps the override's name, and isn't added a second time.
    anchorage_location("356ec741", 34.839059469352883, 128.42069569869318),
    # The anchorage the overrides remove.
    anchorage_location("10000001", 1.5655633705233914e-06, 6.6617493735929531e-06),
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
    """Runs the pipeline on `rows` (shaped like the anchorage locations query's output)."""

    def read_factory(**kwargs):
        if reads is not None:
            reads.append(kwargs)
        return FakeReadFromBigQuery(elements=rows, **kwargs)

    def write_factory(**kwargs):
        if writes is not None:
            writes.append(kwargs)
        return AssertWritten(check, **kwargs)

    config = dict(
        bq_in_anchorage_locations="project.dataset.anchorage_locations",
        bq_out_named_anchorages="project.dataset.named_anchorages",
        anchorage_overrides=ANCHORAGE_OVERRIDES,
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


def names(row):
    return (row["label"], row["sublabel"], row["iso3"], row["label_source"])


def check_named_anchorages(rows):
    by_s2id = {r["s2id"]: r for r in rows}
    assert names(by_s2id["thunder-bay"]) == ("THUNDER BAY", None, "CAN", "WPI_ports")
    assert names(by_s2id["pacific"]) == ("SOMEWHERE", None, None, "top_destination")
    assert names(by_s2id["pacific-2"]) == ("pacific-2", None, None, "top_destination")
    assert names(by_s2id["east-china-sea"]) == (
        "east-china-sea", None, "CHN", "china_s2id_override"
    )
    assert names(by_s2id["356ec741"]) == ("TONGYEONG", None, "KOR", "anchorage_overrides")
    assert "10000001" not in by_s2id

    # The input's stats are kept, and the overrides' anchorages have none.
    assert by_s2id["thunder-bay"]["total_visits"] == 10
    assert by_s2id["thunder-bay"]["unique_stationary_ssvid"] == 3
    assert by_s2id["thunder-bay"]["unique_stationary_fishing_ssvid"] == 1
    assert by_s2id["356ec741"]["total_visits"] == 10
    assert names(by_s2id["1a5e4eaf"]) == (
        "POINTE NOIRE", "POINTE NOIRE OFFSHORE", "COG", "anchorage_overrides"
    )
    assert by_s2id["1a5e4eaf"]["total_visits"] is None

    # 5 input anchorages, plus the overrides but the 2 the input already has.
    assert len(rows) == 5 + N_OVERRIDES - 2
    # Documents current behavior: s2ids aren't unique, since several rows of
    # anchorage_overrides.csv fall in the same S2 cell, and each one is added.
    assert len(by_s2id) < len(rows)
    assert set(rows[0]) == {f["name"] for f in schemas.get_schema("named_anchorages.json")}


def test_run_names_the_anchorage_locations_and_adds_the_overrides(bq_clients, bq_client_factory):
    # The one test that runs the pipeline in Beam: the transforms' own tests cover each case.
    reads, writes = [], []

    run_pipeline(
        ANCHORAGE_LOCATIONS,
        check_named_anchorages,
        reads=reads,
        writes=writes,
        bq_client_factory=bq_client_factory,
    )

    (read,) = reads
    assert "FROM\n    `project.dataset.anchorage_locations`" in read["query"]
    assert read["bigquery_job_labels"] == {"environment": "development", "stage": "anchorages"}
    (write,) = writes
    assert write["table"] == "project.dataset.named_anchorages"
    assert write["write_disposition"] == beam.io.BigQueryDisposition.WRITE_TRUNCATE
    assert write["create_disposition"] == beam.io.BigQueryDisposition.CREATE_NEVER
    assert [f["name"] for f in write["schema"]["fields"]] == [
        f["name"] for f in schemas.get_schema("named_anchorages.json")
    ]
    # The output table is described after writing it.
    (client,) = bq_clients
    (updated, fields), _ = client.update_table.call_args
    assert fields == ["description", "labels"]
    assert "NAMED ANCHORAGES" in updated.description
    assert "project.dataset.anchorage_locations" in updated.description
    assert updated.labels == {"environment": "development", "stage": "anchorages"}


def test_output_table_is_not_partitioned():
    table_config = NamedAnchoragesTableConfig(table_id="project.dataset.output")

    params = table_config.to_bigquery_params()

    assert params.get("partition_field") is None
    assert not params.get("clustering_fields")


def test_anchorage_locations_query_renders():
    query = AnchorageLocationsQuery(source_anchorage_locations="LOCATIONS")

    rendered = query.render()

    assert "FROM\n    `LOCATIONS`" in rendered
    assert "s2id," in rendered
    assert "stationary_fishing_ssvid_days" in rendered
