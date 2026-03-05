#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project)"
if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "gcloud project not set. Run: gcloud config set project YOUR_PROJECT_ID" >&2
  exit 1
fi

ZONE="us-central1-a"
REGION="us-central1"

REPO_URL="https://github.com/sanjask11/CC-HW2.git"

BUCKET_NAME="san-hw2-cc"
PAGES_PREFIX="html-pages"
PORT="8080"

TOPIC="forbidden-requests"
SUBSCRIPTION="forbidden-requests-sub"

SERVER_VM="hw4-server"
REPORTER_VM="hw4-reporter"
CLIENT_VM="hw4-client"

SERVER_SA="hw4-server-sa"
REPORTER_SA="hw4-reporter-sa"
SERVER_SA_EMAIL="${SERVER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"
REPORTER_SA_EMAIL="${REPORTER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

ADDR_NAME="hw4-server-ip"
FW_NAME="hw4-allow-${PORT}"

echo "Enabling APIs..."
gcloud services enable compute.googleapis.com pubsub.googleapis.com logging.googleapis.com storage.googleapis.com >/dev/null

echo "Creating service accounts (if missing)..."
gcloud iam service-accounts create "${SERVER_SA}" --display-name="HW4 Server SA" >/dev/null 2>&1 || true
gcloud iam service-accounts create "${REPORTER_SA}" --display-name="HW4 Reporter SA" >/dev/null 2>&1 || true

echo "Ensuring bucket exists..."
if ! gcloud storage buckets describe "gs://${BUCKET_NAME}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${BUCKET_NAME}" --location=US --uniform-bucket-level-access >/dev/null
  gcloud storage buckets update "gs://${BUCKET_NAME}" --update-labels=hw=4,owner=setup_sh >/dev/null
fi

echo "Creating Pub/Sub topic + subscription..."
gcloud pubsub topics create "${TOPIC}" >/dev/null 2>&1 || true
gcloud pubsub subscriptions create "${SUBSCRIPTION}" --topic="${TOPIC}" >/dev/null 2>&1 || true

echo "Setting minimal IAM..."
# Server SA: read objects, write logs, publish to topic
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVER_SA_EMAIL}" \
  --role="roles/logging.logWriter" >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVER_SA_EMAIL}" \
  --role="roles/pubsub.publisher" >/dev/null

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${SERVER_SA_EMAIL}" \
  --role="roles/storage.objectViewer" >/dev/null


gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${REPORTER_SA_EMAIL}" \
  --role="roles/logging.logWriter" >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${REPORTER_SA_EMAIL}" \
  --role="roles/pubsub.subscriber" >/dev/null

echo "Reserving static IP (regional)..."
gcloud compute addresses create "${ADDR_NAME}" --region="${REGION}" >/dev/null 2>&1 || true
STATIC_IP="$(gcloud compute addresses describe "${ADDR_NAME}" --region="${REGION}" --format='value(address)')"
echo "Static IP: ${STATIC_IP}"

echo "Creating firewall rule to allow TCP ${PORT}..."
gcloud compute firewall-rules create "${FW_NAME}" \
  --allow="tcp:${PORT}" \
  --direction=INGRESS \
  --source-ranges="0.0.0.0/0" \
  --target-tags="hw4-server" >/dev/null 2>&1 || true

echo "Creating server VM..."
gcloud compute instances create "${SERVER_VM}" \
  --zone="${ZONE}" \
  --machine-type="e2-micro" \
  --image-family="debian-12" --image-project="debian-cloud" \
  --service-account="${SERVER_SA_EMAIL}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --tags="hw4-server" \
  --address="${STATIC_IP}" \
  --metadata="APP=service1,PROJECT_ID=${PROJECT_ID},REPO_URL=${REPO_URL},BUCKET=${BUCKET_NAME},PAGES_PREFIX=${PAGES_PREFIX},PORT=${PORT},TOPIC=${TOPIC},SUBSCRIPTION=${SUBSCRIPTION}" \
  --metadata-from-file startup-script="startup.sh" \
  >/dev/null 2>&1 || true

echo "Creating reporter VM..."
gcloud compute instances create "${REPORTER_VM}" \
  --zone="${ZONE}" \
  --machine-type="e2-micro" \
  --image-family="debian-12" --image-project="debian-cloud" \
  --service-account="${REPORTER_SA_EMAIL}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --metadata="APP=service2,PROJECT_ID=${PROJECT_ID},REPO_URL=${REPO_URL},TOPIC=${TOPIC},SUBSCRIPTION=${SUBSCRIPTION}" \
  --metadata-from-file startup-script="startup.sh" \
  >/dev/null 2>&1 || true

echo "Creating client VM..."
gcloud compute instances create "${CLIENT_VM}" \
  --zone="${ZONE}" \
  --machine-type="e2-micro" \
  --image-family="debian-12" --image-project="debian-cloud" \
  --tags="hw4-client" \
  --metadata=startup-script='#! /bin/bash
apt-get update -y
apt-get install -y python3 git
' \
  >/dev/null 2>&1 || true

echo "DONE"
echo "Server URL: http://${STATIC_IP}:${PORT}/"
