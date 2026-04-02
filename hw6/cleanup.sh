#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export CLOUDSDK_CORE_DISABLE_PROMPTS=1

PROJECT_ID="primal-ivy-485619-r6"
ML_VM="hw6-ml-vm"
DB_INSTANCE="hw6-db"

echo "Setting project..."
gcloud config set project "${PROJECT_ID}" >/dev/null

echo "Deleting ML VM (all zones)..."
for Z in us-central1-a us-central1-b us-central1-c us-central1-f \
          us-east1-b us-east1-c us-east1-d \
          us-east4-b us-east4-c \
          us-west1-b us-west1-c; do
  gcloud compute instances delete "${ML_VM}" \
    --zone="${Z}" --quiet >/dev/null 2>&1 || true
done

echo "Stopping Cloud SQL instance..."
gcloud sql instances patch "${DB_INSTANCE}" \
  --activation-policy=NEVER \
  --quiet >/dev/null 2>&1 || true

echo "Revoking application-default auth..."
gcloud auth application-default revoke \
  --quiet >/dev/null 2>&1 || true

echo "Cleanup complete."
