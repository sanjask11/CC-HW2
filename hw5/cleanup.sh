#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project)"
ZONE="us-central1-a"
REGION="us-central1"

TOPIC="forbidden-requests"
SUBSCRIPTION="forbidden-requests-sub"

SERVER_VM="hw5-server"
REPORTER_VM="hw5-reporter"
CLIENT_VM_1="hw5-client-1"
CLIENT_VM_2="hw5-client-2"

SERVER_SA="hw5-server-sa"
REPORTER_SA="hw5-reporter-sa"
CLIENT_SA="hw5-client-sa"

ADDR_NAME="hw5-server-ip"
FW_NAME="hw5-allow-8080"

DB_INSTANCE="hw5-db"

echo "Stopping VMs..."
gcloud compute instances stop "${SERVER_VM}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true
gcloud compute instances stop "${REPORTER_VM}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true
gcloud compute instances stop "${CLIENT_VM_1}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true
gcloud compute instances stop "${CLIENT_VM_2}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true

echo "Stopping database..."
gcloud sql instances patch "${DB_INSTANCE}" --activation-policy=NEVER --quiet >/dev/null 2>&1 || true

echo "Done."
