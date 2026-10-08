import pytest


class _StopAfterPipelineInit(Exception):
    pass


@pytest.fixture
def capture_pipeline_init(mocker):
    """Runs a Beam pipeline's run() only up to its Pipeline(...) call, and returns that mock.

    TEMPORARY: the legacy Beam pipelines (name_anchorages, thin_port_messages) build their
    DAG inline in run(), with no way to inject fake sources/sinks.
    Patching Pipeline alone isn't enough: the DAG then gets built against a Mock, and once a
    plain list of those Mocks is piped into e.g. beam.Flatten(), Beam wraps it in an implicit
    pipeline of its own and actually runs it on the local runner (Prism) -- spinning up a real
    job that fails at WriteToBigQuery and leaks gRPC thread errors into the test output.

    So the patched Pipeline raises right away instead (a Mock still records a call that raises),
    and run() never gets past it. Drop this once those pipelines are migrated to
    gfw-common's PipelineFactory + LinearDagFactory, like pipe-gaps' raw_gaps, and tested
    end-to-end with mock_bq_clients=True instead.
    """

    def _capture(target, run, config):
        mock_pipeline_cls = mocker.patch(target, side_effect=_StopAfterPipelineInit)
        with pytest.raises(_StopAfterPipelineInit):
            run(config)

        return mock_pipeline_cls

    return _capture
