from __future__ import absolute_import

from apache_beam.options.pipeline_options import PipelineOptions


class AnchorageOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        required.add_argument('--message_table', required=True,
                            help='Messages table to pull data from')
        required.add_argument('--segments_table', required=True,
                            help='Segments table to pull data from')
        required.add_argument('--output_table', required=True,
                            help='Output table to write results to.')
        required.add_argument('--start_date', required=True,
                            help="First date to generate visits.")
        required.add_argument('--end_date', required=True,
                            help="Last date (inclusive) to generate visits.")
        required.add_argument('--config', required=True,
                            help="path to configuration file")
        required.add_argument('--fishing_ssvid_list', required=True,
                            help='location of list of newline separated fishing vessel ids')
