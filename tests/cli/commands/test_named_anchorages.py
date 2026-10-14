import argparse

import pytest

from pipe_anchorages.cli import main


BASE_ARGS = [
    "named-anchorages",
    "--bq-in-anchorage-locations", "project.dataset.anchorage_locations",
    "--bq-out-named-anchorages", "project.dataset.named_anchorages",
    "--labels", "environment=development", "stage=anchorages",
]


def test_cli_executes_run_with_mock_bq_clients():
    # The pipeline really builds and runs its Beam DAG, with the BigQuery source/sink swapped for
    # gfw-common's fakes and the in-process FnApiRunner (DirectRunner would pick Prism, which
    # runs as a subprocess and stages an sdist of the package in the working directory).
    main.run(
        [*BASE_ARGS, "--mock-bq-clients", "--project", "test-project", "--runner", "FnApiRunner"]
    )


@pytest.mark.parametrize("flag", ["--bq-in-anchorage-locations", "--bq-out-named-anchorages"])
def test_cli_requires_argument(flag):
    args = list(BASE_ARGS)
    i = args.index(flag)
    del args[i:i + 2]

    with pytest.raises(argparse.ArgumentTypeError, match="Missing required arguments"):
        main.run(args)
