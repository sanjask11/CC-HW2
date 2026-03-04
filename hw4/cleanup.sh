#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project 2>/dev/null)"
REGION="us-central1"
ZONE="us-central1-a"

TOPIC="forbidden-requests"
SUB="forbidden-requests-sub"

ADDR_NAME="hw4-server-ip"
FW_RULE="hw4-allow-8080"

SA_SVC1_NAME="hw4-svc1"
SA_SVC2_NAME="hw4-svc2"
SA_SVC1="${SA_SVC1_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
SA_SVC2="${SA_SVC2_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

VM_SERVER="hw4-server"
VM_CLIENT="hw4-client"
VM_SVC2="hw4-service2"

echo "[cleanup] PROJECT_ID=${PROJECT_ID}"


gcloud compute instances delete "${VM_SERVER}" --zone "${ZONE}" --project "${PROJECT_ID}" --quiet || true
gcloud compute instances delete "${VM_CLIENT}" --zone "${ZONE}" --project "${PROJECT_ID}" --quiet || true
gcloud compute instances delete "${VM_SVC2}" --zone "${ZONE}" --project "${PROJECT_ID}" --quiet || true


gcloud compute firewall-rules delete "${FW_RULE}" --project "${PROJECT_ID}" --quiet || true


gcloud compute addresses delete "${ADDR_NAME}" --region "${REGION}" --project "${PROJECT_ID}" --quiet || true


gcloud pubsub subscriptions delete "${SUB}" --project "${PROJECT_ID}" --quiet || true
gcloud pubsub topics delete "${TOPIC}" --project "${PROJECT_ID}" --quiet || true


gcloud iam service-accounts delete "${SA_SVC1}" --project "${PROJECT_ID}" --quiet || true
gcloud iam service-accounts delete "${SA_SVC2}" --project "${PROJECT_ID}" --quiet || true

echo "[cleanup] done"
