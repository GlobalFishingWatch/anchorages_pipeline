from __future__ import absolute_import
import sys

# Suppress a spurious warning that happens when you import apache_beam
from . import logging_monkeypatch
from .options.logging_options import validate_options
from .options.logging_options import LoggingOptions

from .options.name_anchorage_options import NameAnchorageOptions


def run(args=None):
    options = validate_options(
        args=args, option_classes=[LoggingOptions, NameAnchorageOptions]
    )

    options.view_as(LoggingOptions).configure_logging()

    from pipe_anchorages import name_anchorages_pipeline

    return name_anchorages_pipeline.run(options)


if __name__ == "__main__":
    sys.exit(run(args=sys.argv))
