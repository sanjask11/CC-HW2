#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export CLOUDSDK_CORE_DISABLE_PROMPTS=1


PROJECT_ID="primal-ivy-485619-r6"

ZONE="us-central1-a"
REGION="us-central1"

BUCKET_NAME="san-hw2-cc"
TOPIC="forbidden-requests"
SUBSCRIPTION="forbidden-requests-sub"

SERVER_VM="hw5-server"
REPORTER_VM="hw5-reporter"
CLIENT_VM="hw5-client"

SERVER_SA="hw5-server-sa"
REPORTER_SA="hw5-reporter-sa"
CLIENT_SA="hw5-client-sa"

ADDR_NAME="hw5-server-ip"
FW_NAME="hw5-allow-8080"

DB_INSTANCE="hw5-db"

echo "Setting gcloud project..."
gcloud config set project "${PROJECT_ID}" >/dev/null

echo "Deleting VMs..."
gcloud compute instances delete "${SERVER_VM}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true
gcloud compute instances delete "${REPORTER_VM}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true
gcloud compute instances delete "${CLIENT_VM}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true

echo "Deleting firewall rule..."
gcloud compute firewall-rules delete "${FW_NAME}" --quiet >/dev/null 2>&1 || true

echo "Releasing static IP..."
gcloud compute addresses delete "${ADDR_NAME}" --region="${REGION}" --quiet >/dev/null 2>&1 || true

echo "Deleting Pub/Sub resources..."
gcloud pubsub subscriptions delete "${SUBSCRIPTION}" --quiet >/dev/null 2>&1 || true
gcloud pubsub topics delete "${TOPIC}" --quiet >/dev/null 2>&1 || true

echo "Stopping Cloud SQL instance (do NOT delete)..."
if gcloud sql instances describe "${DB_INSTANCE}" >/dev/null 2>&1; then
  gcloud sql instances patch "${DB_INSTANCE}" --activation-policy=NEVER --quiet >/dev/null 2>&1 || true
fi

echo "Deleting service accounts..."
gcloud iam service-accounts delete "${SERVER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" --quiet >/dev/null 2>&1 || true
gcloud iam service-accounts delete "${REPORTER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" --quiet >/dev/null 2>&1 || true
gcloud iam service-accounts delete "${CLIENT_SA}@${PROJECT_ID}.iam.gserviceaccount.com" --quiet >/dev/null 2>&1 || true

echo "Deleting bucket ONLY if it was created by setup.sh..."
if gcloud storage buckets describe "gs://${BUCKET_NAME}" >/dev/null 2>&1; then
  LABELS="$(gcloud storage buckets describe "gs://${BUCKET_NAME}" --format='value(labels)')"
  if [[ "${LABELS}" == *"hw=5"* && "${LABELS}" == *"owner=setup_sh"* ]]; then
    gcloud storage rm -r "gs://${BUCKET_NAME}" >/dev/null 2>&1 || true
  fi
fi

echo "Revoking application-default auth if present..."
gcloud auth application-default revoke --quiet >/dev/null 2>&1 || true

echo "DONE"