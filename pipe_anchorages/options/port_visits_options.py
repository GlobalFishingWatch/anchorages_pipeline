from __future__ import absolute_import
from apache_beam.options.pipeline_options import PipelineOptions

class PortVisitsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        required.add_argument('--events_table', required=True,
                            help='Name of events table (BQ)')
        required.add_argument('--vessel_id_table', required=True,
                            help='Name of table mapping vessel_id to seg_id (BQ). '
                            'Should have one vessel_id per seg_id, e.g. the `segment_info` table.')
        required.add_argument('--output_table', required=True,
                            help='Output table (BQ) to write results to.')
        required.add_argument('--start_date', required=True,
                            help="First date (inclusive) to include in visits")
        required.add_argument('--end_date', required=True,
                            help="Last date (inclusive) to generate visits")

        optional.add_argument('--compat_output_table',
                            help='Output table (BQ) to write backwards compatible results.')
        optional.add_argument('--bad_segs_table',
                            help='table of containing segment ids of bad segments')
        optional.add_argument('--max_inter_seg_dist_nm', default=60, type=float,
                            help='Segments more than this distance apart will not be joined when creating visits')
        optional.add_argument('--wait_for_job', default=False, action='store_true',
                            help='Wait until the job finishes before returning.')

