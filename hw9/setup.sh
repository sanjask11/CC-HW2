#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project)"
if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "gcloud project not set. Run: gcloud config set project YOUR_PROJECT_ID" >&2
  exit 1
fi

REGION="us-central1"
ZONE="us-central1-a"
CLUSTER_NAME="hw9-gke-cluster"

BUCKET_NAME="san-hw2-cc"
TOPIC="forbidden-requests"
SUBSCRIPTION="forbidden-requests-sub"

IMAGE_NAME_SERVICE1="gcr.io/${PROJECT_ID}/hw9-service1"
IMAGE_TAG="latest"

REPORTER_VM="hw9-reporter-vm"
REPORTER_SA="hw9-reporter-sa"
REPORTER_SA_EMAIL="${REPORTER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

REPO_URL="https://github.com/sanjask11/CC-HW2.git"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

log_step() {
  log "========== $* =========="
}

log_step "Enabling required Google Cloud APIs"
gcloud services enable \
  container.googleapis.com \
  compute.googleapis.com \
  storage.googleapis.com \
  pubsub.googleapis.com \
  logging.googleapis.com \
  cloudresourcemanager.googleapis.com \
  artifactregistry.googleapis.com \
  iam.googleapis.com \
  cloudbuild.googleapis.com \
  --quiet >/dev/null

log_step "Ensuring bucket exists"
if ! gcloud storage buckets describe "gs://${BUCKET_NAME}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${BUCKET_NAME}" \
    --location=US \
    --uniform-bucket-level-access \
    --project="${PROJECT_ID}" >/dev/null
fi

log_step "Creating Pub/Sub resources"
gcloud pubsub topics create "${TOPIC}" --project="${PROJECT_ID}" >/dev/null 2>&1 || true
gcloud pubsub subscriptions create "${SUBSCRIPTION}" \
  --topic="${TOPIC}" \
  --project="${PROJECT_ID}" >/dev/null 2>&1 || true

log_step "Creating GKE cluster"
if gcloud container clusters describe "${CLUSTER_NAME}" \
  --zone="${ZONE}" \
  --project="${PROJECT_ID}" >/dev/null 2>&1; then
  log "Cluster already exists"
else
  gcloud container clusters create "${CLUSTER_NAME}" \
    --zone="${ZONE}" \
    --num-nodes="2" \
    --machine-type="e2-standard-4" \
    --enable-ip-alias \
    --logging=SYSTEM \
    --monitoring=SYSTEM \
    --project="${PROJECT_ID}" >/dev/null
fi

log_step "Configuring kubectl"
gcloud container clusters get-credentials "${CLUSTER_NAME}" \
  --zone="${ZONE}" \
  --project="${PROJECT_ID}" >/dev/null

log_step "Granting GKE node service account permissions"
PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
NODE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${NODE_SA}" \
  --role="roles/logging.logWriter" \
  --quiet >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${NODE_SA}" \
  --role="roles/pubsub.publisher" \
  --quiet >/dev/null

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${NODE_SA}" \
  --role="roles/storage.objectViewer" \
  --quiet >/dev/null

log_step "Building and pushing Docker image for service1 with Cloud Build"
gcloud builds submit "${SCRIPT_DIR}" \
  --tag "${IMAGE_NAME_SERVICE1}:${IMAGE_TAG}" \
  --project="${PROJECT_ID}"

log_step "Rendering Kubernetes manifests"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TEMP_DIR}"' EXIT

cp "${SCRIPT_DIR}/kube-configmap.yaml" "${TEMP_DIR}/"
cp "${SCRIPT_DIR}/kube-service1.yaml" "${TEMP_DIR}/"

sed -i.bak "s|PROJECT_ID|${PROJECT_ID}|g" "${TEMP_DIR}/kube-configmap.yaml"
sed -i.bak "s|PROJECT_ID|${PROJECT_ID}|g" "${TEMP_DIR}/kube-service1.yaml"

log_step "Deploying service1 to GKE"
kubectl apply -f "${TEMP_DIR}/kube-configmap.yaml"
kubectl apply -f "${TEMP_DIR}/kube-service1.yaml"

log "Waiting for service1 rollout"
kubectl rollout status deployment/hw9-service1 --timeout=5m

log_step "Creating VM for service2"
gcloud iam service-accounts create "${REPORTER_SA}" \
  --display-name="HW9 Reporter SA" \
  --project="${PROJECT_ID}" >/dev/null 2>&1 || true

sleep 15

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${REPORTER_SA_EMAIL}" \
  --role="roles/pubsub.subscriber" \
  --quiet >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${REPORTER_SA_EMAIL}" \
  --role="roles/logging.logWriter" \
  --quiet >/dev/null

gcloud compute instances create "${REPORTER_VM}" \
  --zone="${ZONE}" \
  --machine-type="e2-micro" \
  --image-family="debian-12" \
  --image-project="debian-cloud" \
  --service-account="${REPORTER_SA_EMAIL}" \
  --scopes="https://www.googleapis.com/auth/cloud-platform" \
  --metadata="REPO_URL=${REPO_URL},PROJECT_ID=${PROJECT_ID},SUBSCRIPTION=${SUBSCRIPTION}" \
  --metadata-from-file startup-script="${SCRIPT_DIR}/startup-reporter.sh" \
  >/dev/null 2>&1 || true

log_step "Deployment complete"

EXTERNAL_IP="$(kubectl get svc hw9-service1 -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)"
REPORTER_POD_STATUS="$(kubectl get pods -l app=hw9-service1 --no-headers 2>/dev/null || true)"

log "Service1 external IP: ${EXTERNAL_IP:-pending}"
log "Service1 pods:"
log "${REPORTER_POD_STATUS:-pending}"
log "Useful commands:"
log "  kubectl get pods"
log "  kubectl get svc"
log "  kubectl logs deployment/hw9-service1"
log "  gcloud compute ssh ${REPORTER_VM} --zone ${ZONE} --command 'sudo journalctl -u hw9-reporter.service -n 50 --no-pager'"