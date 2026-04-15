#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project)"
if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "Set your gcloud project first." >&2
  exit 1
fi

REGION="us-central1"
ZONE_A="us-central1-a"
ZONE_B="us-central1-b"
PORT="8080"

REPO_URL="https://github.com/sanjask11/CC-HW2.git"
BUCKET_NAME="san-hw2-cc"
PAGES_PREFIX="html-pages"

SERVER_A="hw8-server-a"
SERVER_B="hw8-server-b"
CLIENT_VM="hw8-client"

SERVER_SA="hw8-server-sa"
CLIENT_SA="hw8-client-sa"
SERVER_SA_EMAIL="${SERVER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"
CLIENT_SA_EMAIL="${CLIENT_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

HC_NAME="hw8-health-check"
TP_NAME="hw8-target-pool"
ADDR_NAME="hw8-lb-ip"
FWD_RULE="hw8-forwarding-rule"
FW_NAME="hw8-allow-8080"

echo "Enabling APIs..."
gcloud services enable \
  compute.googleapis.com \
  logging.googleapis.com \
  storage.googleapis.com >/dev/null

echo "Creating service accounts..."
gcloud iam service-accounts create "${SERVER_SA}" --display-name="HW8 Server SA" >/dev/null 2>&1 || true
gcloud iam service-accounts create "${CLIENT_SA}" --display-name="HW8 Client SA" >/dev/null 2>&1 || true

echo "Granting minimal IAM..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVER_SA_EMAIL}" \
  --role="roles/logging.logWriter" >/dev/null

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${SERVER_SA_EMAIL}" \
  --role="roles/storage.objectViewer" >/dev/null

echo "Firewall..."
gcloud compute firewall-rules create "${FW_NAME}" \
  --allow="tcp:${PORT}" \
  --direction=INGRESS \
  --source-ranges="0.0.0.0/0" \
  --target-tags="hw8-server" >/dev/null 2>&1 || true

echo "Creating backend VMs..."
gcloud compute instances create "${SERVER_A}" \
  --zone="${ZONE_A}" \
  --machine-type="e2-micro" \
  --image-family="debian-12" --image-project="debian-cloud" \
  --service-account="${SERVER_SA_EMAIL}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --tags="hw8-server" \
  --metadata="APP=service1,PROJECT_ID=${PROJECT_ID},REPO_URL=${REPO_URL},BUCKET=${BUCKET_NAME},PAGES_PREFIX=${PAGES_PREFIX},PORT=${PORT}" \
  --metadata-from-file startup-script="startup.sh"

gcloud compute instances create "${SERVER_B}" \
  --zone="${ZONE_B}" \
  --machine-type="e2-micro" \
  --image-family="debian-12" --image-project="debian-cloud" \
  --service-account="${SERVER_SA_EMAIL}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --tags="hw8-server" \
  --metadata="APP=service1,PROJECT_ID=${PROJECT_ID},REPO_URL=${REPO_URL},BUCKET=${BUCKET_NAME},PAGES_PREFIX=${PAGES_PREFIX},PORT=${PORT}" \
  --metadata-from-file startup-script="startup.sh"

echo "Creating optional client VM..."
gcloud compute instances create "${CLIENT_VM}" \
  --zone="${ZONE_A}" \
  --machine-type="e2-micro" \
  --image-family="debian-12" --image-project="debian-cloud" \
  --service-account="${CLIENT_SA_EMAIL}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --metadata="APP=client,PROJECT_ID=${PROJECT_ID},REPO_URL=${REPO_URL},PORT=${PORT}" \
  --metadata-from-file startup-script="startup.sh" >/dev/null 2>&1 || true

echo "Creating health check..."
gcloud compute http-health-checks create "${HC_NAME}" \
  --port="${PORT}" \
  --request-path="/healthz" >/dev/null 2>&1 || true

echo "Creating target pool..."
gcloud compute target-pools create "${TP_NAME}" \
  --region="${REGION}" \
  --http-health-check="${HC_NAME}" >/dev/null 2>&1 || true

echo "Adding instances to target pool..."
gcloud compute target-pools add-instances "${TP_NAME}" \
  --instances="${SERVER_A},${SERVER_B}" \
  --instances-zone="${ZONE_A}" >/dev/null 2>&1 || true

gcloud compute target-pools add-instances "${TP_NAME}" \
  --instances="${SERVER_B}" \
  --instances-zone="${ZONE_B}" >/dev/null 2>&1 || true

echo "Reserving static IP..."
gcloud compute addresses create "${ADDR_NAME}" --region="${REGION}" >/dev/null 2>&1 || true
STATIC_IP="$(gcloud compute addresses describe "${ADDR_NAME}" --region="${REGION}" --format='value(address)')"

echo "Creating forwarding rule..."
gcloud compute forwarding-rules create "${FWD_RULE}" \
  --region="${REGION}" \
  --ports="${PORT}" \
  --address="${STATIC_IP}" \
  --target-pool="${TP_NAME}" >/dev/null 2>&1 || true

echo "Load balancer IP: ${STATIC_IP}"