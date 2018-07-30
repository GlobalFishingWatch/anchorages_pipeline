from __future__ import absolute_import
from apache_beam.options.pipeline_options import PipelineOptions

class NameAnchorageOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        required.add_argument('--input_table', required=True,
                             help='Input table to pull data from')
        required.add_argument('--output_table', required=True,
                            help='Output table to write results to.')
        required.add_argument('--config', required=True, 
                            help="path to configuration file")

        optional.add_argument('--shapefile', default='EEZ_land_v2_201410.shp',
                            help="path to configuration file")



    

# docker-compose run name_anchorages \
#                   --name nameanchorages \
#                   --input-table machine_learning_dev_ttl_120d.unnamed_anchorages \
#                   --output-table machine_learning_dev_ttl_120d.named_anchorages_test \
#                   --config ./name_anchorages_cfg.yaml



# def parse_command_line_args():
#     parser = argparse.ArgumentParser()

#     parser.add_argument('--name', required=True, 
#                         help='Name to prefix output and job name if not otherwise specified')
#     # TODO: Replace
#     parser.add_argument('--output-table', 
#                         help='Output table to write results to.')
#     parser.add_argument('--input-table', required=True,
#                         help='Input anchorage table to pull data from')
#     parser.add_argument('--config-path', required=True)
#     parser.add_argument('--shapefile-path', default='EEZ_land_v2_201410.shp',
#                         help="path to configuration file")

#     known_args, pipeline_args = parser.parse_known_args()

#     if known_args.output_table is None:
#         known_args.output_table = 'machine_learning_dev_ttl_30d.anchorages_{}'.format(known_args.name)

#     cmn.add_pipeline_defaults(pipeline_args, known_args.name)

#     # We use the save_main_session option because one or more DoFn's in this
#     # workflow rely on global context (e.g., a module imported at module level).
#     pipeline_options = PipelineOptions(pipeline_args)
#     cmn.check_that_pipeline_args_consumed(pipeline_options)
#     pipeline_options.view_as(SetupOptions).save_main_session = True

#     return known_args, pipeline_options