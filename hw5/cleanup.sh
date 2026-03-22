#!/bin/bash

set -e


PROJECT_ID="primal-ivy-485619-r6"
REGION="us-central1"
ZONE="us-central1-a"
DB_INSTANCE_NAME="hw5-database"

echo "========================================="
echo "Starting HW5 Cleanup - Deleting All Resources"
echo "========================================="

gcloud config set project $PROJECT_ID

echo ""
echo "Step 1: Deleting VM instances..."
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
echo "Step 2: Deleting Cloud SQL database..."
gcloud sql instances delete $DB_INSTANCE_NAME \
    --project=$PROJECT_ID \
    --quiet 2>/dev/null || echo "Database doesn't exist"

echo ""
echo "Step 3: Releasing static IP address..."
gcloud compute addresses delete hw5-server-ip \
    --region=$REGION \
    --project=$PROJECT_ID \
    --quiet 2>/dev/null || echo "Static IP doesn't exist"

echo ""
echo "Step 4: Deleting firewall rule..."
gcloud compute firewall-rules delete allow-http-8080 \
    --project=$PROJECT_ID \
    --quiet 2>/dev/null || echo "Firewall rule doesn't exist"

echo ""
echo "Step 5: Deleting Pub/Sub subscription..."
gcloud pubsub subscriptions delete forbidden-requests-sub \
    --project=$PROJECT_ID \
    --quiet 2>/dev/null || echo "Subscription doesn't exist"

echo ""
echo "Step 6: Deleting Pub/Sub topic..."
gcloud pubsub topics delete forbidden-requests \
    --project=$PROJECT_ID \
    --quiet 2>/dev/null || echo "Topic doesn't exist"

echo ""
echo "Step 7: Deleting service accounts..."
gcloud iam service-accounts delete hw5-webserver-sa@${PROJECT_ID}.iam.gserviceaccount.com \
    --project=$PROJECT_ID \
    --quiet 2>/dev/null || echo "Web server service account doesn't exist"

gcloud iam service-accounts delete hw5-reporter-sa@${PROJECT_ID}.iam.gserviceaccount.com \
    --project=$PROJECT_ID \
    --quiet 2>/dev/null || echo "Reporter service account doesn't exist"

echo ""
echo "Step 8: Revoking Application Default Credentials (if present)..."
# This ensures no ADC is left over that could interfere
rm -f ~/.config/gcloud/application_default_credentials.json 2>/dev/null || true

echo ""
echo "========================================="
echo "Cleanup Complete!"
echo "========================================="
