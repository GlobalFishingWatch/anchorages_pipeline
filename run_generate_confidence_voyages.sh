#!/usr/bin/env bash
#
# Runs the generate_confidence_voyages step via docker-compose.
#
# NOTE: unlike port_visits / thin_port_messages, this step is NOT a Dataflow
# pipeline. It runs BigQuery jobs directly through the BigQuery client, so there
# is no runner, no worker sizing, and no --service_account_email. The BigQuery
# jobs run as the LAUNCHER identity (the ADC in the gcp volume). To run them as
# a specific service account, authenticate the gcp volume as that SA (e.g. ADC
# impersonation) before running.
#
# Prereqs:
#   - docker + docker compose installed
#   - GCP auth volume populated once:  make docker-gcp
#   - The dev image is built:          make docker-build
#
# Usage:
#   ./run_generate_confidence_voyages.sh
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Job configuration
# ---------------------------------------------------------------------------
PROJECT="world-fishing-827"

# Source port_visits table and confidence-filtered output table.
SOURCE="world-fishing-827.vi_928_quick_fix_3.port_visits"
OUTPUT="world-fishing-827.vi_928_quick_fix_3.voyages_c4"

# Minimal confidence to detect voyages. One of {2,3,4}.
MIN_CONFIDENCE="4"

# Labels to audit BigQuery costs. Must be valid JSON (parsed with json.loads).
# Same labels as port_visits, but this step takes a single JSON object rather
# than repeated --labels=k=v flags.
LABELS='{"project":"quick-fix-3","mode":"backfill","stage":"port-visits-v4"}'

# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------
docker compose run --rm pipeline \
  generate_confidence_voyages \
  --source="${SOURCE}" \
  --min_confidence="${MIN_CONFIDENCE}" \
  --output="${OUTPUT}" \
  --labels="${LABELS}" \
  --project="${PROJECT}"
