from pipe_anchorages import thin_port_messages_pipeline
from pipe_anchorages.options.logging_options import LoggingOptions, validate_options
from pipe_anchorages.options.thin_port_messages_options import ThinPortMessagesOptions
import sys


def run(args=None):
    options = validate_options(
        args=args, option_classes=[LoggingOptions, ThinPortMessagesOptions]
    )

    options.view_as(LoggingOptions).configure_logging()

    return thin_port_messages_pipeline.run(options)


if __name__ == "__main__":
    sys.exit(run(args=sys.argv))
