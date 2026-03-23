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

SERVER_SA_EMAIL="${SERVER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"
REPORTER_SA_EMAIL="${REPORTER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"
CLIENT_SA_EMAIL="${CLIENT_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

ADDR_NAME="hw5-server-ip"
FW_NAME="hw5-allow-8080"

DB_INSTANCE="hw5-db"
DB_NAME="requestsdb"

echo "Deleting VMs..."
gcloud compute instances delete "${SERVER_VM}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true
gcloud compute instances delete "${REPORTER_VM}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true
gcloud compute instances delete "${CLIENT_VM_1}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true
gcloud compute instances delete "${CLIENT_VM_2}" --zone="${ZONE}" --quiet >/dev/null 2>&1 || true

echo "Deleting firewall rule..."
gcloud compute firewall-rules delete "${FW_NAME}" --quiet >/dev/null 2>&1 || true

echo "Releasing static IP..."
gcloud compute addresses delete "${ADDR_NAME}" --region="${REGION}" --quiet >/dev/null 2>&1 || true

echo "Deleting Pub/Sub resources..."
gcloud pubsub subscriptions delete "${SUBSCRIPTION}" --quiet >/dev/null 2>&1 || true
gcloud pubsub topics delete "${TOPIC}" --quiet >/dev/null 2>&1 || true

echo "Deleting Cloud SQL database and instance..."
gcloud sql databases delete "${DB_NAME}" --instance="${DB_INSTANCE}" --quiet >/dev/null 2>&1 || true
gcloud sql instances delete "${DB_INSTANCE}" --quiet >/dev/null 2>&1 || true

echo "Deleting service accounts..."
gcloud iam service-accounts delete "${SERVER_SA_EMAIL}" --quiet >/dev/null 2>&1 || true
gcloud iam service-accounts delete "${REPORTER_SA_EMAIL}" --quiet >/dev/null 2>&1 || true
gcloud iam service-accounts delete "${CLIENT_SA_EMAIL}" --quiet >/dev/null 2>&1 || true

echo "Revoking ADC if present..."
gcloud auth application-default revoke --quiet >/dev/null 2>&1 || true

echo "DONE"
