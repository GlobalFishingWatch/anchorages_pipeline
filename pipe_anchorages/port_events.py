from __future__ import absolute_import
import sys

# Suppress a spurious warning that happens when you import apache_beam
from . import logging_monkeypatch
from .options.logging_options import validate_options
from .options.logging_options import LoggingOptions

from .options.port_events_options import PortEventsOptions


def run(args=None):
    options = validate_options(
        args=args, option_classes=[LoggingOptions, PortEventsOptions]
    )

    options.view_as(LoggingOptions).configure_logging()

    from pipe_anchorages import port_events_pipeline

    return port_events_pipeline.run(options)


if __name__ == "__main__":
    sys.exit(run(args=sys.argv))
