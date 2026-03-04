#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project)"
ZONE="us-central1-a"
REGION="us-central1"

BUCKET_NAME="san-hw2-cc"

TOPIC="forbidden-requests"
SUBSCRIPTION="forbidden-requests-sub"

SERVER_VM="hw4-server"
REPORTER_VM="hw4-reporter"

SERVER_SA="hw4-server-sa"
REPORTER_SA="hw4-reporter-sa"

ADDR_NAME="hw4-server-ip"
FW_NAME="hw4-allow-8080"

echo "Deleting VMs..."
gcloud compute instances delete "${SERVER_VM}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true
gcloud compute instances delete "${REPORTER_VM}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true

echo "Deleting firewall rule..."
gcloud compute firewall-rules delete "${FW_NAME}" --quiet >/dev/null 2>&1 || true

echo "Releasing static IP..."
gcloud compute addresses delete "${ADDR_NAME}" --region="${REGION}" --quiet >/dev/null 2>&1 || true

echo "Deleting Pub/Sub resources..."
gcloud pubsub subscriptions delete "${SUBSCRIPTION}" --quiet >/dev/null 2>&1 || true
gcloud pubsub topics delete "${TOPIC}" --quiet >/dev/null 2>&1 || true

echo "Deleting service accounts..."
gcloud iam service-accounts delete "${SERVER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" --quiet >/dev/null 2>&1 || true
gcloud iam service-accounts delete "${REPORTER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" --quiet >/dev/null 2>&1 || true

echo "Deleting bucket ONLY if it was created by setup.sh (labels hw=4, owner=setup_sh)..."
if gcloud storage buckets describe "gs://${BUCKET_NAME}" >/dev/null 2>&1; then
  LABELS="$(gcloud storage buckets describe "gs://${BUCKET_NAME}" --format='value(labels)')"
  if [[ "${LABELS}" == *"hw=4"* && "${LABELS}" == *"owner=setup_sh"* ]]; then
    gcloud storage rm -r "gs://${BUCKET_NAME}" >/dev/null 2>&1 || true
  fi
fi

echo "Revoking ADC if present..."
gcloud auth application-default revoke --quiet >/dev/null 2>&1 || true

echo "DONE"
