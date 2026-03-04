#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project 2>/dev/null)"
if [[ -z "${PROJECT_ID}" ]]; then
  echo "No active gcloud project. Run: gcloud config set project <PROJECT_ID>"
  exit 1
fi

REGION="us-central1"
ZONE="us-central1-a"

# Names
TOPIC="forbidden-requests"
SUB="forbidden-requests-sub"

BUCKET="san-hw2-cc"
PAGES_PREFIX="html-pages"
PORT="8080"

ADDR_NAME="hw4-server-ip"
FW_RULE="hw4-allow-8080"

SA_SVC1_NAME="hw4-svc1"
SA_SVC2_NAME="hw4-svc2"
SA_SVC1="${SA_SVC1_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
SA_SVC2="${SA_SVC2_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

VM_SERVER="hw4-server"
VM_CLIENT="hw4-client"
VM_SVC2="hw4-service2"


REPO_URL="https://github.com/sanjask11/CC-HW2.git"

echo "[setup] PROJECT_ID=${PROJECT_ID}"


gcloud services enable \
  compute.googleapis.com \
  iam.googleapis.com \
  pubsub.googleapis.com \
  logging.googleapis.com \
  storage.googleapis.com \
  iamcredentials.googleapis.com \
  --project "${PROJECT_ID}"


if ! gcloud pubsub topics describe "${TOPIC}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud pubsub topics create "${TOPIC}" --project "${PROJECT_ID}"
fi

if ! gcloud pubsub subscriptions describe "${SUB}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud pubsub subscriptions create "${SUB}" \
    --topic "${TOPIC}" \
    --project "${PROJECT_ID}"
fi


if ! gcloud iam service-accounts describe "${SA_SVC1}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud iam service-accounts create "${SA_SVC1_NAME}" --project "${PROJECT_ID}"
fi

if ! gcloud iam service-accounts describe "${SA_SVC2}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud iam service-accounts create "${SA_SVC2_NAME}" --project "${PROJECT_ID}"
fi


gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${SA_SVC1}" \
  --role="roles/storage.objectViewer" >/dev/null

gcloud pubsub topics add-iam-policy-binding "${TOPIC}" \
  --member="serviceAccount:${SA_SVC1}" \
  --role="roles/pubsub.publisher" \
  --project "${PROJECT_ID}" >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA_SVC1}" \
  --role="roles/logging.logWriter" >/dev/null


gcloud pubsub subscriptions add-iam-policy-binding "${SUB}" \
  --member="serviceAccount:${SA_SVC2}" \
  --role="roles/pubsub.subscriber" \
  --project "${PROJECT_ID}" >/dev/null

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${SA_SVC2}" \
  --role="roles/storage.objectAdmin" >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA_SVC2}" \
  --role="roles/logging.logWriter" >/dev/null


if ! gcloud compute addresses describe "${ADDR_NAME}" --region "${REGION}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud compute addresses create "${ADDR_NAME}" --region "${REGION}" --project "${PROJECT_ID}"
fi

SERVER_IP="$(gcloud compute addresses describe "${ADDR_NAME}" --region "${REGION}" --project "${PROJECT_ID}" --format='value(address)')"
echo "[setup] SERVER_IP=${SERVER_IP}"


if ! gcloud compute firewall-rules describe "${FW_RULE}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud compute firewall-rules create "${FW_RULE}" \
    --allow "tcp:${PORT}" \
    --direction INGRESS \
    --target-tags "hw4-server" \
    --source-ranges "0.0.0.0/0" \
    --project "${PROJECT_ID}"
fi


if ! gcloud compute instances describe "${VM_SERVER}" --zone "${ZONE}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud compute instances create "${VM_SERVER}" \
    --zone "${ZONE}" \
    --machine-type "e2-micro" \
    --address "${SERVER_IP}" \
    --tags "hw4-server" \
    --service-account "${SA_SVC1}" \
    --scopes "https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/pubsub,https://www.googleapis.com/auth/logging.write" \
    --metadata-from-file startup-script=startup.sh \
    --metadata "APP=server,REPO_URL=${REPO_URL},PROJECT_ID=${PROJECT_ID},BUCKET=${BUCKET},PAGES_PREFIX=${PAGES_PREFIX},PORT=${PORT},TOPIC=${TOPIC}"
fi


if ! gcloud compute instances describe "${VM_CLIENT}" --zone "${ZONE}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud compute instances create "${VM_CLIENT}" \
    --zone "${ZONE}" \
    --machine-type "e2-micro" \
    --no-service-account \
    --no-scopes \
    --metadata-from-file startup-script=startup.sh \
    --metadata "APP=client,REPO_URL=${REPO_URL},PROJECT_ID=${PROJECT_ID},BUCKET=${BUCKET},PAGES_PREFIX=${PAGES_PREFIX},PORT=${PORT}"
fi


if ! gcloud compute instances describe "${VM_SVC2}" --zone "${ZONE}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud compute instances create "${VM_SVC2}" \
    --zone "${ZONE}" \
    --machine-type "e2-micro" \
    --service-account "${SA_SVC2}" \
    --scopes "https://www.googleapis.com/auth/devstorage.read_write,https://www.googleapis.com/auth/pubsub,https://www.googleapis.com/auth/logging.write" \
    --metadata-from-file startup-script=startup.sh \
    --metadata "APP=service2,REPO_URL=${REPO_URL},PROJECT_ID=${PROJECT_ID},BUCKET=${BUCKET},SUBSCRIPTION_ID=${SUB},LOG_PREFIX=service2-logs"
fi

echo "[setup] done"
echo "[setup] Server URL examples:"
echo "  http://${SERVER_IP}:${PORT}/${PAGES_PREFIX}/0.html"
echo "  http://${SERVER_IP}:${PORT}/${PAGES_PREFIX}/does_not_exist_99999.html"
