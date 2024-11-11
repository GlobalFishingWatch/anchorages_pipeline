from apache_beam.options.pipeline_options import GoogleCloudOptions, StandardOptions
from apache_beam.runners import PipelineState
from pipe_anchorages.voyages.options import VoyagesOptions
from pipe_anchorages.voyages.transforms.read_source import ReadSource
from pipe_anchorages.voyages.transforms.group import GroupByVessels
from pipe_anchorages.voyages.transforms.create import CreateVoyages
from pipe_anchorages.voyages.transforms.sink import WriteSink

import apache_beam as beam
import logging


success_states = set(
    [
        PipelineState.DONE,
        PipelineState.RUNNING,
        PipelineState.UNKNOWN,
        PipelineState.PENDING,
    ]
)


class VoyagesPipeline:
    def __init__(self, options):
        self.options = options
        params = options.view_as(VoyagesOptions)
        cloud_options = options.view_as(GoogleCloudOptions)

        self.pipeline = beam.Pipeline(options=options)
        self.sink = WriteSink(params, cloud_options)
        (
            self.pipeline
            | ReadSource(params.source_table, params.first_table_date, cloud_options)
            | GroupByVessels()
            | CreateVoyages()
            | self.sink
        )

    def run(self):
        result = self.pipeline.run()
        if (
            self.options.view_as(VoyagesOptions).wait_for_job
            or self.options.view_as(StandardOptions).runner == "DirectRunner"
        ):
            result.wait_until_finish()
            if result.state == PipelineState.DONE:
                self.sink.update_description()
                self.sink.update_labels()

        logging.info("Returning with result.state=%s" % result.state)
        return 0 if result.state in success_states else 1
