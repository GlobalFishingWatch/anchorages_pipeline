from __future__ import absolute_import

import sys

from .options.logging_options import LoggingOptions, validate_options
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
