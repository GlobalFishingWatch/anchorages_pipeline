from pipe_anchorages import logging_monkeypatch, name_anchorages_pipeline
from pipe_anchorages.options.logging_options import LoggingOptions, validate_options
from pipe_anchorages.options.name_anchorage_options import NameAnchorageOptions
import sys


def run(args=None):
    options = validate_options(
        args=args, option_classes=[LoggingOptions, NameAnchorageOptions]
    )

    options.view_as(LoggingOptions).configure_logging()

    return name_anchorages_pipeline.run(options)


if __name__ == "__main__":
    sys.exit(run(args=sys.argv))
