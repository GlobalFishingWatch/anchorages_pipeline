#!/usr/bin/env bash
#
# Runs the port_visits pipeline on Dataflow via docker-compose.
#
# Prereqs:
#   - docker + docker compose installed
#   - GCP auth volume populated once:  make docker-gcp
#     (creates the "gcp" volume and runs `gcloud auth application-default login`)
#   - The dev image is built:          make docker-build
#
# Auth model:
#   - The LAUNCHER (this container) submits the job using the ADC in the gcp
#     volume. That identity needs roles/iam.serviceAccountUser on the SA below.
#   - The DATAFLOW WORKERS run as SERVICE_ACCOUNT_EMAIL.
#
# Usage:
#   ./run_port_visits.sh
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Job configuration
# ---------------------------------------------------------------------------
PROJECT="world-fishing-827"
REGION="us-central1"

# Service account the Dataflow workers run as (must have BQ/GCS access).
SERVICE_ACCOUNT_EMAIL="research-and-development@world-fishing-827.iam.gserviceaccount.com"

# Dataflow job name: must match [a-z]([-a-z0-9]*[a-z0-9])? (no underscores/uppercase).
# Pass one in as $1 to override.
JOB_NAME="${1:-anchorages-port-visits}"

# GCS staging/temp bucket used by the BigQuery read export and load steps.
TEMP_LOCATION="gs://pipe-temp-us-central-ttl7/dataflow_temp"

# Data tables / date range.
START_DATE="2012-01-01"
END_DATE="2025-12-31"
VESSEL_ID_TABLE="world-fishing-827.prj_entity_hull.entity_epoch_v20260801"
THINNED_MESSAGE_TABLE="gfw-int-ais-datalake.port_visits_v1.raw_port_events"
OUTPUT_TABLE="world-fishing-827.vi_928_quick_fix_3.port_visits"
BAD_SEGS='(SELECT DISTINCT seg_id FROM `global-fishing-watch.pipe_ais_v5_published.segs_activity` WHERE overlapping_and_short )'

# Worker sizing.
MAX_NUM_WORKERS=200
DISK_SIZE_GB=100

# Prebuild config. Dataflow builds a custom SDK worker container (base image +
# this package) on Cloud Build and pushes it to Artifact Registry. This replaces
# the --setup_file sdist path, which fails here because the image is installed
# with --no-deps and lacks setuptools/build.
#
# BASE_SDK_IMAGE must be FULLY-QUALIFIED (docker.io/...) and match the beam
# version pinned in requirements.txt (apache-beam==2.69.0) and the Dockerfile.
BASE_SDK_IMAGE="docker.io/apache/beam_python3.12_sdk:2.69.0"
DOCKER_REGISTRY_PUSH_URL="us-central1-docker.pkg.dev/world-fishing-827/development/pipe-anchorages-vi-929"

# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------
docker compose run --rm pipeline \
  port_visits \
  --start_date="${START_DATE}" \
  --end_date="${END_DATE}" \
  --vessel_id_table="${VESSEL_ID_TABLE}" \
  --thinned_message_table="${THINNED_MESSAGE_TABLE}" \
  --output_table="${OUTPUT_TABLE}" \
  --bad_segs="${BAD_SEGS}" \
  --labels=project=quick-fix-3 \
  --labels=mode=backfill \
  --labels=stage=port-visits-v4 \
  --runner=dataflow \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --service_account_email="${SERVICE_ACCOUNT_EMAIL}" \
  --job_name="${JOB_NAME}" \
  --temp_location="${TEMP_LOCATION}" \
  --max_num_workers="${MAX_NUM_WORKERS}" \
  --disk_size_gb="${DISK_SIZE_GB}" \
  --setup_file=./setup.py \
  --requirements_file=requirements.txt \
  --prebuild_sdk_container_engine=cloud_build \
  --docker_registry_push_url="${DOCKER_REGISTRY_PUSH_URL}" \
  --sdk_container_image="${BASE_SDK_IMAGE}" \
  --sdk_location=container \
  --wait_for_job
