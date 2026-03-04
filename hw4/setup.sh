#!/bin/bash
set -euo pipefail

PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
ZONE="us-central1-a"
PORT="8080"

REPO_URL="https://github.com/sanjask11/CC-HW2.git"

BUCKET="san-hw2-cc"
PAGES_PREFIX="html-pages"
TOPIC="forbidden-requests"
SUBSCRIPTION="forbidden-requests-sub"

SERVER_NAME="hw4-server"
REPORTER_NAME="hw4-reporter"
CLIENT_NAME="hw4-client"
CREATE_CLIENT_VM="true"

SA_SVC1="hw4-svc1"
SA_SVC2="hw4-svc2"

ADDR_NAME="hw4-server-ip"
FW_NAME="hw4-allow-8080"
SERVER_TAG="hw4-server"

# APIs
gcloud services enable compute.googleapis.com iam.googleapis.com logging.googleapis.com storage.googleapis.com pubsub.googleapis.com

# Pub/Sub topic + subscription
if ! gcloud pubsub topics describe "$TOPIC" >/dev/null 2>&1; then
  gcloud pubsub topics create "$TOPIC"
fi

if ! gcloud pubsub subscriptions describe "$SUBSCRIPTION" >/dev/null 2>&1; then
  gcloud pubsub subscriptions create "$SUBSCRIPTION" --topic="$TOPIC"
fi

# Service accounts
if ! gcloud iam service-accounts describe "$SA_SVC1@$PROJECT_ID.iam.gserviceaccount.com" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_SVC1" --display-name="HW4 Service1 Account"
fi

if ! gcloud iam service-accounts describe "$SA_SVC2@$PROJECT_ID.iam.gserviceaccount.com" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_SVC2" --display-name="HW4 Service2 Account"
fi

# IAM bindings (idempotent enough; duplicates are ok in grading contexts)
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_SVC1@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer" >/dev/null

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_SVC1@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/logging.logWriter" >/dev/null

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_SVC1@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher" >/dev/null

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_SVC2@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/logging.logWriter" >/dev/null

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_SVC2@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/pubsub.subscriber" >/dev/null

# Static IP
if ! gcloud compute addresses describe "$ADDR_NAME" --region="$REGION" >/dev/null 2>&1; then
  gcloud compute addresses create "$ADDR_NAME" --region="$REGION"
fi

SERVER_IP=$(gcloud compute addresses describe "$ADDR_NAME" --region="$REGION" --format="get(address)")
echo "SERVER_IP=$SERVER_IP"

# Firewall
if ! gcloud compute firewall-rules describe "$FW_NAME" >/dev/null 2>&1; then
  gcloud compute firewall-rules create "$FW_NAME" \
    --allow "tcp:${PORT}" \
    --target-tags "$SERVER_TAG" \
    --direction INGRESS
fi

# Create server VM
if ! gcloud compute instances describe "$SERVER_NAME" --zone="$ZONE" >/dev/null 2>&1; then
  gcloud compute instances create "$SERVER_NAME" \
    --zone="$ZONE" \
    --machine-type="e2-micro" \
    --address="$ADDR_NAME" \
    --tags="$SERVER_TAG" \
    --service-account="$SA_SVC1@$PROJECT_ID.iam.gserviceaccount.com" \
    --scopes="https://www.googleapis.com/auth/cloud-platform" \
    --metadata="APP=service1,REPO_URL=$REPO_URL,BUCKET=$BUCKET,PAGES_PREFIX=$PAGES_PREFIX,TOPIC=$TOPIC,SUBSCRIPTION=$SUBSCRIPTION,PORT=$PORT" \
    --metadata-from-file startup-script=startup.sh
fi

# Create reporter VM
if ! gcloud compute instances describe "$REPORTER_NAME" --zone="$ZONE" >/dev/null 2>&1; then
  gcloud compute instances create "$REPORTER_NAME" \
    --zone="$ZONE" \
    --machine-type="e2-micro" \
    --service-account="$SA_SVC2@$PROJECT_ID.iam.gserviceaccount.com" \
    --scopes="https://www.googleapis.com/auth/cloud-platform" \
    --metadata="APP=service2,REPO_URL=$REPO_URL,BUCKET=$BUCKET,PAGES_PREFIX=$PAGES_PREFIX,TOPIC=$TOPIC,SUBSCRIPTION=$SUBSCRIPTION,PORT=$PORT" \
    --metadata-from-file startup-script=startup.sh
fi

# Optional client VM
if [ "$CREATE_CLIENT_VM" = "true" ]; then
  if ! gcloud compute instances describe "$CLIENT_NAME" --zone="$ZONE" >/dev/null 2>&1; then
    gcloud compute instances create "$CLIENT_NAME" \
      --zone="$ZONE" \
      --machine-type="e2-micro"
  fi
fi

echo "Done."
echo "Server URL: http://$SERVER_IP:$PORT/"
