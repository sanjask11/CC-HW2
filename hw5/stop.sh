#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project)"
ZONE="us-central1-a"
DB_INSTANCE="hw5-db"

gcloud compute instances stop hw5-server --zone="${ZONE}" --quiet >/dev/null 2>&1 || true
gcloud compute instances stop hw5-reporter --zone="${ZONE}" --quiet >/dev/null 2>&1 || true
gcloud compute instances stop hw5-client --zone="${ZONE}" --quiet >/dev/null 2>&1 || true

gcloud sql instances patch "${DB_INSTANCE}" --activation-policy=NEVER --quiet >/dev/null 2>&1 || true

echo "Stopped VMs and requested DB stop."