from pipe_anchorages.voyages.pipeline import VoyagesPipeline
from pipe_anchorages.options.logging_options import LoggingOptions, validate_options
from pipe_anchorages.voyages.options import VoyagesOptions
import sys


def run(args=None):
    options = validate_options(args=args, option_classes=[LoggingOptions, VoyagesOptions])

    options.view_as(LoggingOptions).configure_logging()

    voyages = VoyagesPipeline(options)
    return voyages.run()


if __name__ == "__main__":
    sys.exit(run(args=sys.argv))
