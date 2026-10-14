import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions


@pytest.fixture
def pipeline():
    """A Beam pipeline on the in-process FnApiRunner, run when the `with` block exits."""
    return beam.Pipeline(options=PipelineOptions(runner="FnApiRunner"))
