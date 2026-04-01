#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export CLOUDSDK_CORE_DISABLE_PROMPTS=1

PROJECT_ID="primal-ivy-485619-r6"
ZONE="us-central1-a"

ML_VM="hw6-ml-vm"
DB_INSTANCE="hw6-db"

echo "Setting project..."
gcloud config set project "${PROJECT_ID}" >/dev/null

echo "Deleting ML VM..."
gcloud compute instances delete "${ML_VM}" \
  --zone="${ZONE}" \
  --quiet >/dev/null 2>&1 || true

echo "Stopping Cloud SQL instance..."
gcloud sql instances patch "${DB_INSTANCE}" \
  --activation-policy=NEVER \
  --quiet >/dev/null 2>&1 || true

echo "Revoking application-default auth..."
gcloud auth application-default revoke \
  --quiet >/dev/null 2>&1 || true

echo "Cleanup complete."