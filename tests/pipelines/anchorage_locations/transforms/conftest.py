import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions


@pytest.fixture
def pipeline():
    """A Beam pipeline on Prism, Beam's local runner, run when the `with` block exits."""
    return beam.Pipeline(options=PipelineOptions(runner="PrismRunner"))
