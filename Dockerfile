# ---------------------------------------------------------------------------------------
# BUILDER
# ---------------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS builder

VOLUME ["/root/.config"]

RUN apt-get update && \
    apt-get install -y --no-install-recommends libexpat1 && \
    rm -rf /var/lib/apt/lists/*

# Use uv for high-speed installs
COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1

# Install dependencies BEFORE copying source so an edit under src/
# doesn't invalidate the cache of the (expensive) requirements-install layer.
COPY pyproject.toml requirements.txt README.md MANIFEST.in ./
RUN uv pip install --system --upgrade pip && \
    uv pip install --system build && \
    uv pip install --system --prefix=/install -r requirements.txt

COPY src ./src
RUN uv pip install --system --prefix=/install --no-deps .

# ---------------------------------------------------------------------------------------
# PRODUCTION IMAGE
# ---------------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS prod

ENV PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends libexpat1 && \
    rm -rf /var/lib/apt/lists/*

# COPY PYTHON PACKAGES
COPY --from=builder /install /usr/local

# APACHE BEAM INTEGRATION
# IMPORTANT: this version must match apache-beam[gcp] in pyproject.toml.
COPY --from=apache/beam_python3.12_sdk:2.76.0 /opt/apache/beam /opt/apache/beam
ENTRYPOINT ["/opt/apache/beam/boot"]

WORKDIR /opt/project

# ---------------------------------------------------------------------------------------
# DEVELOPMENT IMAGE
# ---------------------------------------------------------------------------------------
FROM builder AS dev

RUN apt-get update && \
    apt-get install -y --no-install-recommends make && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project

COPY . .
RUN uv pip install --system -e .[lint,dev,build] && \
    uv pip install --system -r requirements-test.txt

# ---------------------------------------------------------------------------------------
# TEST IMAGE
# ---------------------------------------------------------------------------------------
FROM prod AS test

COPY ./requirements-test.txt .
RUN pip install -r requirements-test.txt

# The tests run Beam on Prism, a binary Beam downloads from GitHub on first use. Fetch it here,
# so the tests don't download it on every run.
RUN python -c "from apache_beam.options.pipeline_options import PipelineOptions; \
from apache_beam.runners.portability.prism_runner import PrismJobServer as S; \
server = S(PipelineOptions()); server.local_bin(server.path_to_binary(), S.BIN_CACHE)"

COPY ./tests ./tests

# Suppress all warnings during tests
# To see/address warnings, run tests in your development environment.
ENV PYTHONWARNINGS=ignore
