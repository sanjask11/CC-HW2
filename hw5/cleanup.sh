#!/bin/bash

set -e

PROJECT_ID="primal-ivy-485619-r6"
REGION="us-central1"
ZONE="us-central1-a"
DB_INSTANCE_NAME="hw5-database"

echo "========================================="
echo "Starting HW5 Cleanup"
echo "========================================="

gcloud config set project $PROJECT_ID

echo ""
echo "Step 1: Stopping Cloud SQL database..."
gcloud sql instances patch $DB_INSTANCE_NAME \
    --activation-policy=NEVER \
    --project=$PROJECT_ID 2>/dev/null || echo "Database already stopped or doesn't exist"

echo ""
echo "Step 2: Deleting VM instances..."
gcloud compute instances delete hw5-server \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --quiet 2>/dev/null || echo "hw5-server doesn't exist"

gcloud compute instances delete hw5-reporter \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --quiet 2>/dev/null || echo "hw5-reporter doesn't exist"

gcloud compute instances delete hw5-client \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --quiet 2>/dev/null || echo "hw5-client doesn't exist"

echo ""
echo "========================================="
echo "Cleanup Complete!"
echo "========================================="
echo ""
echo "Resources stopped/deleted:"
echo "  - Cloud SQL instance: $DB_INSTANCE_NAME (stopped, not deleted)"
echo "  - VM: hw5-server (deleted)"
echo "  - VM: hw5-reporter (deleted)"
echo "  - VM: hw5-client (deleted)"
echo ""
echo "Resources NOT deleted (reusable):"
echo "  - Static IP address"
echo "  - Firewall rules"
echo "  - Service accounts"
echo "  - Pub/Sub topic and subscription"
echo "  - Storage bucket"
echo ""
echo "To completely remove everything, run:"
echo "  gcloud sql instances delete $DB_INSTANCE_NAME"
echo "  gcloud compute addresses delete hw5-server-ip --region=$REGION"
echo "  gcloud compute firewall-rules delete allow-http-8080"
echo ""
