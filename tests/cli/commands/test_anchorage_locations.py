import argparse
import datetime

import pytest

from gfw.common.config import PipelineConfigError
from pipe_anchorages.cli import main


BASE_ARGS = [
    "anchorage-locations",
    "--bq-in-messages", "project.dataset.messages",
    "--bq-out-anchorage-locations", "project.dataset.anchorage_locations",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-07",
    "--gcs-in-fishing-ssvids", "gs://bucket/fishing_mmsi.txt",
    "--labels", "environment=development", "stage=anchorages",
]


def test_cli_executes_run_with_the_config(monkeypatch):
    configs = []
    monkeypatch.setattr(
        "pipe_anchorages.cli.commands.anchorage_locations.run",
        lambda config, **kwargs: configs.append(config),
    )

    main.run(BASE_ARGS)

    (config,) = configs
    assert config.bq_in_messages == "project.dataset.messages"
    assert config.bq_out_anchorage_locations == "project.dataset.anchorage_locations"
    assert config.gcs_in_fishing_ssvids == "gs://bucket/fishing_mmsi.txt"
    assert config.start_date == datetime.date(2024, 1, 1)
    assert config.end_date == datetime.date(2024, 1, 7)
    assert config.min_positions == 200
    assert config.min_unique_vessels == 20
    assert config.stationary_period_min_duration_minutes == 720
    assert config.stationary_period_max_distance_km == 0.5
    assert config.labels == {"environment": "development", "stage": "anchorages"}


@pytest.mark.parametrize(
    "flag",
    [
        "--bq-in-messages",
        "--bq-out-anchorage-locations",
        "--start-date",
        "--end-date",
        "--gcs-in-fishing-ssvids",
    ],
)
def test_cli_requires_argument(flag):
    args = list(BASE_ARGS)
    i = args.index(flag)
    del args[i:i + 2]

    with pytest.raises(argparse.ArgumentTypeError, match="Missing required arguments"):
        main.run(args)


def test_cli_rejects_an_invalid_date():
    args = list(BASE_ARGS)
    args[args.index("2024-01-07")] = "07/01/2024"

    with pytest.raises(SystemExit):
        main.run(args)


def test_cli_rejects_an_empty_date_range():
    args = list(BASE_ARGS)
    args[args.index("2024-01-07")] = "2024-01-01"

    with pytest.raises(PipelineConfigError, match=r"end_date .* must be after start_date"):
        main.run(args)
