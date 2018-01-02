from __future__ import absolute_import
import sys

# Suppress a spurious warning that happens when you import apache_beam
from pipe_tools.beam import logging_monkeypatch
from pipe_tools.options import validate_options
from pipe_tools.options import LoggingOptions

from .options.port_visits_options import PortVisitsOptions


def run(args=None):
    options = validate_options(args=args, option_classes=[LoggingOptions, PortVisitsOptions])

    options.view_as(LoggingOptions).configure_logging()

    from . import port_visits_pipeline

    return port_visits_pipeline.run(options)

if __name__ == '__main__':
    sys.exit(run(args=sys.argv))

