#!/bin/bash
set -euo pipefail

PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
ZONE="us-central1-a"

SERVER_NAME="hw4-server"
REPORTER_NAME="hw4-reporter"
CLIENT_NAME="hw4-client"

ADDR_NAME="hw4-server-ip"
FW_NAME="hw4-allow-8080"

TOPIC="forbidden-requests"
SUBSCRIPTION="forbidden-requests-sub"

SA_SVC1="hw4-svc1"
SA_SVC2="hw4-svc2"

# VMs
gcloud compute instances delete "$SERVER_NAME" --zone="$ZONE" --quiet >/dev/null 2>&1 || true
gcloud compute instances delete "$REPORTER_NAME" --zone="$ZONE" --quiet >/dev/null 2>&1 || true
gcloud compute instances delete "$CLIENT_NAME" --zone="$ZONE" --quiet >/dev/null 2>&1 || true

# Firewall
gcloud compute firewall-rules delete "$FW_NAME" --quiet >/dev/null 2>&1 || true

# Static IP
gcloud compute addresses delete "$ADDR_NAME" --region="$REGION" --quiet >/dev/null 2>&1 || true

# Pub/Sub
gcloud pubsub subscriptions delete "$SUBSCRIPTION" --quiet >/dev/null 2>&1 || true
gcloud pubsub topics delete "$TOPIC" --quiet >/dev/null 2>&1 || true

# Service accounts
gcloud iam service-accounts delete "$SA_SVC1@$PROJECT_ID.iam.gserviceaccount.com" --quiet >/dev/null 2>&1 || true
gcloud iam service-accounts delete "$SA_SVC2@$PROJECT_ID.iam.gserviceaccount.com" --quiet >/dev/null 2>&1 || true

echo "Cleanup complete."
