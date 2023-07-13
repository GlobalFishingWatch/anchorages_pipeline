from pipe_anchorages import logging_monkeypatch, port_visits_pipeline
from pipe_anchorages.options.logging_options import LoggingOptions, validate_options
from pipe_anchorages.options.port_visits_options import PortVisitsOptions
import sys


def run(args=None):
    options = validate_options(
        args=args, option_classes=[LoggingOptions, PortVisitsOptions]
    )

    options.view_as(LoggingOptions).configure_logging()

    return port_visits_pipeline.run(options)


if __name__ == "__main__":
    sys.exit(run(args=sys.argv))
