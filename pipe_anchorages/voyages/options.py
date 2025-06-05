from apache_beam.options.pipeline_options import PipelineOptions


class VoyagesOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group("Required")
        required.add_argument(
            "--source_table", required=True, help="The port visits table to pull data from"
        )
        required.add_argument(
            "--output_table_c2", required=True, help="Output table to write results to."
        )
        required.add_argument(
            "--output_table_c3", required=True, help="Output table to write results to."
        )
        required.add_argument(
            "--output_table_c4", required=True, help="Output table to write results to."
        )

        optional = parser.add_argument_group("Optional")
        optional.add_argument(
            "--wait_for_job",
            required=False,
            default=False,
            action="store_true",
            help="Wait until the job finishes before returning.",
        )
        optional.add_argument(
            "--first_table_date",
            required=False,
            default="2012-01-01",
            help="The first table date to consider for voyages.",
        )
