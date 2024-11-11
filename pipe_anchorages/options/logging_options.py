import logging
import sys
import six

from apache_beam.options.pipeline_options import PipelineOptions
import argparse

from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import SetupOptions


class LoggingOptions(PipelineOptions):
    log_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
    }

    DEFAULT_LOG_LEVEL = "INFO"

    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        parser.add_argument("--log_file", help="file to send logging output to")

        parser.add_argument(
            "--log_level",
            default=cls.DEFAULT_LOG_LEVEL,
            choices=list(cls.log_levels.keys()),
            help="logging level (default: %(default)s)",
        )

        parser.add_argument(
            "--log_args",
            default=False,
            action="store_true",
            help="Output command line arguments to logging (useful for debugging then you cant get stdout",
        )

    def configure_logging(self):
        args = self._flags or sys.argv
        logger = logging.getLogger()
        if self.log_file:
            logger.addHandler(logging.FileHandler(self.log_file))
        else:
            logging.basicConfig()
        logger.setLevel(self.log_levels[self.log_level])
        if self.log_args:
            logging.info("Running with these command line params")
            for arg in args:
                logging.info("   %s" % arg)


def flatten(struct):
    """
    Creates a flat list of all all items in structured output (dicts, lists, items):
    .. code-block:: python
        >>> sorted(flatten({'a': 'foo', 'b': 'bar'}))
        ['bar', 'foo']
        >>> sorted(flatten(['foo', ['bar', 'troll']]))
        ['bar', 'foo', 'troll']
        >>> flatten('foo')
        ['foo']
        >>> flatten(42)
        [42]
    """
    if struct is None:
        return []
    flat = []
    if isinstance(struct, dict):
        for _, result in six.iteritems(struct):
            flat += flatten(result)
        return flat
    if isinstance(struct, six.string_types):
        return [struct]

    try:
        # if iterable
        iterator = iter(struct)
    except TypeError:
        return [struct]

    for result in iterator:
        flat += flatten(result)
    return flat


def validate_options(args=None, option_classes=None):

    args = args or sys.argv
    option_classes = flatten(option_classes)

    help_flags = ["-h", "--help"]
    help = any(flag in help_flags for flag in args)

    # first check to see if we are using the DirectRunner or the DataflowRunner
    # need to strip out any help params so that we don't exit too early
    nohelp_args = [arg for arg in sys.argv if arg not in help_flags]
    # Parse args just for StandardOptions and see which runner we are using
    local = StandardOptions(nohelp_args).runner in (None, "DirectRunner")

    # make a new parser
    parser = argparse.ArgumentParser()

    # add args for all the options classes that we are using
    for opt in option_classes:
        opt._add_argparse_args(parser)
    StandardOptions._add_argparse_args(parser.add_argument_group("Dataflow Runner"))

    if help or not local:
        GoogleCloudOptions._add_argparse_args(parser.add_argument_group("Dataflow Runtime"))
        WorkerOptions._add_argparse_args(parser.add_argument_group("Dataflow Workers"))
        SetupOptions._add_argparse_args(parser.add_argument_group("Dataflow Setup"))

    # parse all args and trigger help if any required args are missing
    parser.parse_known_args(args)

    return PipelineOptions(args)
