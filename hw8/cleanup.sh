#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project)"
REGION="us-central1"
ZONE_A="us-central1-a"
ZONE_B="us-central1-b"

SERVER_A="hw8-server-a"
SERVER_B="hw8-server-b"
CLIENT_VM="hw8-client"

SERVER_SA="hw8-server-sa"
CLIENT_SA="hw8-client-sa"

HC_NAME="hw8-health-check"
TP_NAME="hw8-target-pool"
ADDR_NAME="hw8-lb-ip"
FWD_RULE="hw8-forwarding-rule"
FW_NAME="hw8-allow-8080"

echo "Deleting VMs..."
gcloud compute instances delete "${SERVER_A}" --zone="${ZONE_A}" --quiet >/dev/null 2>&1 || true
gcloud compute instances delete "${SERVER_B}" --zone="${ZONE_B}" --quiet >/dev/null 2>&1 || true
gcloud compute instances delete "${CLIENT_VM}" --zone="${ZONE_A}" --quiet >/dev/null 2>&1 || true

echo "Deleting forwarding rule..."
gcloud compute forwarding-rules delete "${FWD_RULE}" --region="${REGION}" --quiet >/dev/null 2>&1 || true

echo "Deleting target pool..."
gcloud compute target-pools delete "${TP_NAME}" --region="${REGION}" --quiet >/dev/null 2>&1 || true

echo "Deleting health check..."
gcloud compute http-health-checks delete "${HC_NAME}" --quiet >/dev/null 2>&1 || true

echo "Deleting firewall..."
gcloud compute firewall-rules delete "${FW_NAME}" --quiet >/dev/null 2>&1 || true

echo "Deleting static IP..."
gcloud compute addresses delete "${ADDR_NAME}" --region="${REGION}" --quiet >/dev/null 2>&1 || true

echo "Deleting service accounts..."
gcloud iam service-accounts delete "${SERVER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" --quiet >/dev/null 2>&1 || true
gcloud iam service-accounts delete "${CLIENT_SA}@${PROJECT_ID}.iam.gserviceaccount.com" --quiet >/dev/null 2>&1 || true

echo "Done."