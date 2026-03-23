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
CLIENT_VM_1="hw5-client-1"
CLIENT_VM_2="hw5-client-2"

SERVER_SA="hw5-server-sa"
REPORTER_SA="hw5-reporter-sa"
CLIENT_SA="hw5-client-sa"

SERVER_SA_EMAIL="${SERVER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"
REPORTER_SA_EMAIL="${REPORTER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"
CLIENT_SA_EMAIL="${CLIENT_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

ADDR_NAME="hw5-server-ip"
FW_NAME="hw5-allow-${PORT}"

DB_INSTANCE="hw5-db"
DB_NAME="requestsdb"
DB_USER="hw5user"
DB_PASSWORD="HW5StrongPass123!"
DB_VERSION="MYSQL_8_0"
DB_TIER="db-f1-micro"

echo "Enabling APIs..."
gcloud services enable \
  compute.googleapis.com \
  pubsub.googleapis.com \
  logging.googleapis.com \
  storage.googleapis.com \
  sqladmin.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  eventarc.googleapis.com >/dev/null

echo "Creating service accounts..."
gcloud iam service-accounts create "${SERVER_SA}" --display-name="HW5 Server SA" >/dev/null 2>&1 || true
gcloud iam service-accounts create "${REPORTER_SA}" --display-name="HW5 Reporter SA" >/dev/null 2>&1 || true
gcloud iam service-accounts create "${CLIENT_SA}" --display-name="HW5 Client SA" >/dev/null 2>&1 || true

echo "Creating Pub/Sub topic + subscription..."
gcloud pubsub topics create "${TOPIC}" >/dev/null 2>&1 || true
gcloud pubsub subscriptions create "${SUBSCRIPTION}" --topic="${TOPIC}" >/dev/null 2>&1 || true

echo "Creating Cloud SQL instance if missing..."
gcloud sql instances create "${DB_INSTANCE}" \
  --database-version="${DB_VERSION}" \
  --tier="${DB_TIER}" \
  --region="${REGION}" \
  --root-password="${DB_PASSWORD}" >/dev/null 2>&1 || true

echo "Starting database..."
gcloud sql instances patch "${DB_INSTANCE}" --activation-policy=ALWAYS --quiet >/dev/null

echo "Creating database + DB user..."
gcloud sql databases create "${DB_NAME}" --instance="${DB_INSTANCE}" >/dev/null 2>&1 || true
gcloud sql users create "${DB_USER}" --instance="${DB_INSTANCE}" --password="${DB_PASSWORD}" >/dev/null 2>&1 || true

DB_CONNECTION_NAME="$(gcloud sql instances describe "${DB_INSTANCE}" --format='value(connectionName)')"

echo "Assigning IAM..."

# Server VM permissions
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

# Reporter VM permissions
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

echo "Creating server VM..."
gcloud compute instances create "${SERVER_VM}" \
  --zone="${ZONE}" \
  --machine-type="e2-small" \
  --image-family="debian-12" --image-project="debian-cloud" \
  --service-account="${SERVER_SA_EMAIL}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --tags="hw5-server" \
  --address="${STATIC_IP}" \
  --metadata="APP=service1,PROJECT_ID=${PROJECT_ID},REPO_URL=${REPO_URL},BUCKET=${BUCKET_NAME},PAGES_PREFIX=${PAGES_PREFIX},PORT=${PORT},TOPIC=${TOPIC},SUBSCRIPTION=${SUBSCRIPTION},DB_HOST=127.0.0.1,DB_PORT=3306,DB_USER=${DB_USER},DB_PASSWORD=${DB_PASSWORD},DB_NAME=${DB_NAME},DB_CONNECTION_NAME=${DB_CONNECTION_NAME}" \
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

echo "Creating client VM 1..."
gcloud compute instances create "${CLIENT_VM_1}" \
  --zone="${ZONE}" \
  --machine-type="e2-micro" \
  --image-family="debian-12" --image-project="debian-cloud" \
  --service-account="${CLIENT_SA_EMAIL}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --metadata="startup-script=apt-get update -y && apt-get install -y python3 git curl" \
  >/dev/null 2>&1 || true

echo "Creating client VM 2..."
gcloud compute instances create "${CLIENT_VM_2}" \
  --zone="${ZONE}" \
  --machine-type="e2-micro" \
  --image-family="debian-12" --image-project="debian-cloud" \
  --service-account="${CLIENT_SA_EMAIL}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --metadata="startup-script=apt-get update -y && apt-get install -y python3 git curl" \
  >/dev/null 2>&1 || true

echo "DONE"
echo "Server URL: http://${STATIC_IP}:${PORT}/"
echo "DB connection name: ${DB_CONNECTION_NAME}"
