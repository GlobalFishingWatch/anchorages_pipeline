from __future__ import absolute_import
from apache_beam.options.pipeline_options import PipelineOptions


class NameAnchorageOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group("Required")
        optional = parser.add_argument_group("Optional")

        required.add_argument(
            "--input_table", required=True, help="Input table to pull data from"
        )
        required.add_argument(
            "--output_table", required=True, help="Output table to write results to."
        )
        required.add_argument(
            "--config", required=True, help="path to configuration file"
        )

        optional.add_argument(
            "--shapefile",
            default="EEZ_Land_v3_202030.shp",
            help="path to configuration file",
        )
