import argparse

import pytest

from pipe_anchorages.cli import main


BASE_ARGS = [
    "named-anchorages",
    "--bq-in-anchorage-locations", "project.dataset.anchorage_locations",
    "--bq-out-named-anchorages", "project.dataset.named_anchorages",
    "--labels", "environment=development", "stage=anchorages",
]


def test_cli_executes_run_with_the_config(monkeypatch):
    configs = []
    monkeypatch.setattr(
        "pipe_anchorages.cli.commands.named_anchorages.run",
        lambda config, **kwargs: configs.append(config),
    )

    main.run(BASE_ARGS)

    (config,) = configs
    assert config.bq_in_anchorage_locations == "project.dataset.anchorage_locations"
    assert config.bq_out_named_anchorages == "project.dataset.named_anchorages"
    assert config.shapefile == "EEZ_Land_v3_202030.shp"
    assert config.label_distance_km == 4.0
    assert config.sublabel_distance_km == 1.0
    assert config.anchorage_overrides == "anchorage_overrides.csv"
    assert list(config.reference_ports) == ["peru.csv", "indonesia.csv", "WPI_ports.csv"]
    assert list(config.reference_places) == ["geonames_1000.csv"]


@pytest.mark.parametrize("flag", ["--bq-in-anchorage-locations", "--bq-out-named-anchorages"])
def test_cli_requires_argument(flag):
    args = list(BASE_ARGS)
    i = args.index(flag)
    del args[i:i + 2]

    with pytest.raises(argparse.ArgumentTypeError, match="Missing required arguments"):
        main.run(args)
