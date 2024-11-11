from pipe_anchorages import anchorages_pipeline, logging_monkeypatch
from pipe_anchorages.options.anchorage_options import AnchorageOptions
from pipe_anchorages.options.logging_options import LoggingOptions, validate_options
import sys


def run(args=None):
    options = validate_options(args=args, option_classes=[LoggingOptions, AnchorageOptions])

    options.view_as(LoggingOptions).configure_logging()

    return anchorages_pipeline.run(options)


if __name__ == "__main__":
    sys.exit(run(args=sys.argv))
