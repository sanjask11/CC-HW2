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

SERVER_VM="hw5-server"
REPORTER_VM="hw5-reporter"
CLIENT_VM="hw5-client"

SERVER_SA="hw5-server-sa"
REPORTER_SA="hw5-reporter-sa"
CLIENT_SA="hw5-client-sa"

SERVER_SA_EMAIL="${SERVER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"
REPORTER_SA_EMAIL="${REPORTER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"
CLIENT_SA_EMAIL="${CLIENT_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

ADDR_NAME="hw5-server-ip"
FW_NAME="hw5-allow-${PORT}"

DB_INSTANCE="hw5-db"
DB_NAME="hw5db"
DB_USER="hw5user"
DB_PASSWORD="hw5pass123"

echo "Enabling APIs..."
gcloud services enable \
  compute.googleapis.com \
  pubsub.googleapis.com \
  logging.googleapis.com \
  storage.googleapis.com \
  sqladmin.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  run.googleapis.com \
  eventarc.googleapis.com \
  cloudscheduler.googleapis.com >/dev/null

echo "Creating service accounts..."
gcloud iam service-accounts create "${SERVER_SA}" --display-name="HW5 Server SA" >/dev/null 2>&1 || true
gcloud iam service-accounts create "${REPORTER_SA}" --display-name="HW5 Reporter SA" >/dev/null 2>&1 || true
gcloud iam service-accounts create "${CLIENT_SA}" --display-name="HW5 Client SA" >/dev/null 2>&1 || true

echo "Ensuring bucket exists..."
if ! gcloud storage buckets describe "gs://${BUCKET_NAME}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${BUCKET_NAME}" --location=US --uniform-bucket-level-access >/dev/null
  gcloud storage buckets update "gs://${BUCKET_NAME}" --update-labels=hw=5,owner=setup_sh >/dev/null
fi

echo "Creating Pub/Sub topic + subscription..."
gcloud pubsub topics create "${TOPIC}" >/dev/null 2>&1 || true
gcloud pubsub subscriptions create "${SUBSCRIPTION}" --topic="${TOPIC}" >/dev/null 2>&1 || true

echo "Setting IAM..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVER_SA_EMAIL}" \
  --role="roles/logging.logWriter" >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVER_SA_EMAIL}" \
  --role="roles/pubsub.publisher" >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVER_SA_EMAIL}" \
  --role="roles/cloudsql.client" >/dev/null

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${SERVER_SA_EMAIL}" \
  --role="roles/storage.objectViewer" >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${REPORTER_SA_EMAIL}" \
  --role="roles/logging.logWriter" >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${REPORTER_SA_EMAIL}" \
  --role="roles/pubsub.subscriber" >/dev/null

echo "Reserving static IP..."
gcloud compute addresses create "${ADDR_NAME}" --region="${REGION}" >/dev/null 2>&1 || true
STATIC_IP="$(gcloud compute addresses describe "${ADDR_NAME}" --region="${REGION}" --format='value(address)')"

echo "Creating firewall rule..."
gcloud compute firewall-rules create "${FW_NAME}" \
  --allow="tcp:${PORT}" \
  --direction=INGRESS \
  --source-ranges="0.0.0.0/0" \
  --target-tags="hw5-server" >/dev/null 2>&1 || true

echo "Creating Cloud SQL instance..."
if ! gcloud sql instances describe "${DB_INSTANCE}" >/dev/null 2>&1; then
  gcloud sql instances create "${DB_INSTANCE}" \
    --database-version=MYSQL_8_0 \
    --tier=db-g1-small \
    --region="${REGION}" \
    --storage-size=10GB \
    --storage-type=SSD \
    --availability-type=zonal \
    --backup-start-time=03:00 >/dev/null
fi

echo "Ensuring database is RUNNABLE..."
DB_STATE="$(gcloud sql instances describe "${DB_INSTANCE}" --format='value(state)')"
if [[ "${DB_STATE}" == "STOPPED" ]]; then
  gcloud sql instances patch "${DB_INSTANCE}" --activation-policy=ALWAYS --quiet >/dev/null
fi

echo "Creating database and user..."
gcloud sql databases create "${DB_NAME}" --instance="${DB_INSTANCE}" >/dev/null 2>&1 || true
gcloud sql users create "${DB_USER}" --instance="${DB_INSTANCE}" --password="${DB_PASSWORD}" >/dev/null 2>&1 || true

INSTANCE_CONNECTION_NAME="$(gcloud sql instances describe "${DB_INSTANCE}" --format='value(connectionName)')"

echo "Creating server VM..."
gcloud compute instances create "${SERVER_VM}" \
  --zone="${ZONE}" \
  --machine-type="e2-small" \
  --image-family="debian-12" --image-project="debian-cloud" \
  --service-account="${SERVER_SA_EMAIL}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --tags="hw5-server" \
  --address="${STATIC_IP}" \
  --metadata="APP=service1,PROJECT_ID=${PROJECT_ID},REPO_URL=${REPO_URL},BUCKET=${BUCKET_NAME},PAGES_PREFIX=${PAGES_PREFIX},PORT=${PORT},TOPIC=${TOPIC},SUBSCRIPTION=${SUBSCRIPTION},DB_INSTANCE=${DB_INSTANCE},DB_NAME=${DB_NAME},DB_USER=${DB_USER},DB_PASSWORD=${DB_PASSWORD},INSTANCE_CONNECTION_NAME=${INSTANCE_CONNECTION_NAME}" \
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
  --service-account="${CLIENT_SA_EMAIL}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --tags="hw5-client" \
  --metadata="startup-script=apt-get update -y && apt-get install -y python3 git" \
  >/dev/null 2>&1 || true

echo "DONE"
echo "Server URL: http://${STATIC_IP}:${PORT}/"