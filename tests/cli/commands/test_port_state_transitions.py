import argparse
import datetime

import pytest

from gfw.common.config import PipelineConfigError
from pipe_anchorages.cli import main
from pipe_anchorages.cli.commands.port_state_transitions import PortStateTransitions


BASE_ARGS = [
    "port-state-transitions",
    "--bq-in-named-anchorages", "project.dataset.anchorages",
    "--bq-in-messages", "project.dataset.messages",
    "--bq-out-port-state-transitions", "project.dataset.output",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-07",
    "--labels", "environment=development", "stage=anchorages",
]


def test_cli_executes_run_with_the_config(monkeypatch):
    configs = []
    monkeypatch.setattr(
        "pipe_anchorages.cli.commands.port_state_transitions.run",
        lambda config, **kwargs: configs.append(config),
    )

    main.run(BASE_ARGS)

    (config,) = configs
    assert config.bq_in_named_anchorages == "project.dataset.anchorages"
    assert config.bq_in_messages == "project.dataset.messages"
    assert config.bq_out_port_state_transitions == "project.dataset.output"
    assert config.start_date == datetime.date(2024, 1, 1)
    assert config.end_date == datetime.date(2024, 1, 7)
    assert config.labels == {"environment": "development", "stage": "anchorages"}


@pytest.mark.parametrize(
    "flag",
    [
        "--bq-in-named-anchorages",
        "--bq-in-messages",
        "--bq-out-port-state-transitions",
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


def test_transition_options_are_shared_with_port_visits():
    # port-visits imports and reuses this method rather than redeclaring the
    # same 5 Options, so the two commands can't drift apart. Locking in the
    # exact flag names/defaults here protects that shared contract.
    flags = {opt.flags[0]: opt for opt in PortStateTransitions.transition_options()}

    assert set(flags) == {
        "--anchorage-entry-dist-km",
        "--anchorage-exit-dist-km",
        "--stopping-speed-knots",
        "--starting-speed-knots",
        "--min-anchorage-gap-minutes",
    }
    assert flags["--anchorage-entry-dist-km"].default == 3.0
    assert flags["--anchorage-exit-dist-km"].default == 4.0
    assert flags["--stopping-speed-knots"].default == 0.2
    assert flags["--starting-speed-knots"].default == 0.5
    assert flags["--min-anchorage-gap-minutes"].default == 240.0
