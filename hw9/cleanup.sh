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
REPORTER_VM="hw9-reporter-vm"
REPORTER_SA="hw9-reporter-sa"
REPORTER_SA_EMAIL="${REPORTER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

log_step() {
  log "========== $* =========="
}

log_step "Undeploying Kubernetes resources"
if gcloud container clusters describe "${CLUSTER_NAME}" \
  --zone="${ZONE}" \
  --project="${PROJECT_ID}" >/dev/null 2>&1; then

  gcloud container clusters get-credentials "${CLUSTER_NAME}" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}" >/dev/null

  kubectl delete service hw9-service1 --namespace=default --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl delete deployment hw9-service1 --namespace=default --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl delete configmap hw9-config --namespace=default --ignore-not-found=true >/dev/null 2>&1 || true
fi

log_step "Deleting GKE cluster"
gcloud container clusters delete "${CLUSTER_NAME}" \
  --zone="${ZONE}" \
  --quiet \
  --project="${PROJECT_ID}" >/dev/null 2>&1 || true

log_step "Deleting reporter VM"
gcloud compute instances delete "${REPORTER_VM}" \
  --zone="${ZONE}" \
  --quiet \
  --project="${PROJECT_ID}" >/dev/null 2>&1 || true

log_step "Deleting reporter service account"
gcloud projects remove-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${REPORTER_SA_EMAIL}" \
  --role="roles/logging.logWriter" \
  --quiet >/dev/null 2>&1 || true

gcloud projects remove-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${REPORTER_SA_EMAIL}" \
  --role="roles/pubsub.subscriber" \
  --quiet >/dev/null 2>&1 || true

gcloud iam service-accounts delete "${REPORTER_SA_EMAIL}" \
  --quiet \
  --project="${PROJECT_ID}" >/dev/null 2>&1 || true

log_step "Deleting Pub/Sub resources"
gcloud pubsub subscriptions delete "${SUBSCRIPTION}" \
  --quiet \
  --project="${PROJECT_ID}" >/dev/null 2>&1 || true

gcloud pubsub topics delete "${TOPIC}" \
  --quiet \
  --project="${PROJECT_ID}" >/dev/null 2>&1 || true

log_step "Deleting Docker image"
gcloud container images delete "gcr.io/${PROJECT_ID}/hw9-service1" \
  --quiet \
  --project="${PROJECT_ID}" >/dev/null 2>&1 || true

log "Cleanup complete"