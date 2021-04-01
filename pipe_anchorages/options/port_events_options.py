from __future__ import absolute_import
from apache_beam.options.pipeline_options import PipelineOptions

class PortEventsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        required.add_argument('--anchorage_table', 
                            help='Name of of anchorages table (BQ)')
        required.add_argument('--input_table', required=True,
                            help='Thinned messsage table to pull position messages from')
        required.add_argument('--state_table', required=True,
                            help='Table containing port state on each day')
        required.add_argument('--output_table', required=True,
                            help='Output table (BQ) to write results to.')
        required.add_argument('--start_date', required=True, 
                              help="First date to look for entry/exit events.")
        required.add_argument('--end_date', required=True, 
                            help="Last date (inclusive) to look for entry/exit events.")
        
        optional.add_argument('--config', default='anchorage_cfg.yaml',
                            help="path to configuration file")
        optional.add_argument('--fast_test', action='store_true', 
                            help='limit query size for testing')
   
