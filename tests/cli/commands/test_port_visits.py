import argparse

import pytest

from gfw.common.config import PipelineConfigError
from pipe_anchorages.cli import main


BASE_ARGS = [
    "port-visits",
    "--bq-in-port-state-transitions", "project.dataset.port_state_transitions",
    "--bq-in-segment-info", "project.dataset.segment_info",
    "--bq-out-port-visits", "project.dataset.port_visits",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-07",
    "--labels", "environment=development", "stage=anchorages",
]


def test_cli_executes_run_with_mock_bq_clients():
    # The pipeline really builds and runs its Beam DAG, with the BigQuery source/sink swapped for
    # gfw-common's fakes and the in-process FnApiRunner (DirectRunner would pick Prism, which
    # runs as a subprocess and stages an sdist of the package in the working directory).
    main.run(
        [*BASE_ARGS, "--mock-bq-clients", "--project", "test-project", "--runner", "FnApiRunner"]
    )


@pytest.mark.parametrize(
    "flag",
    [
        "--bq-in-port-state-transitions",
        "--bq-in-segment-info",
        "--bq-out-port-visits",
        "--start-date",
        "--end-date",
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
