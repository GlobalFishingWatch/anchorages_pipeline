#!/usr/bin/env bash

PIPELINE='pipe_anchorages'
PIPELINE_VERSION=$(python -c "import pkg_resources; print pkg_resources.get_distribution('${PIPELINE}').version")