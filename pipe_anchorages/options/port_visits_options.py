from __future__ import absolute_import
from apache_beam.options.pipeline_options import PipelineOptions

class PortVisitsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        required.add_argument('--events_table', 
                            help='Name of of events table (BQ)')
        required.add_argument('--sink_table', required=True,
                            help='Output table (BQ) to write results to.')
        required.add_argument('--start_date', required=True, 
                              help="First date to look for entry/exit events.")
        required.add_argument('--end_date', required=True, 
                            help="Last date (inclusive) to look for entry/exit events.")
        
   
