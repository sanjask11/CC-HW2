#!/bin/bash

set -e  # Exit on any error

# Configuration
PROJECT_ID="primal-ivy-485619-r6"
REGION="us-central1"
ZONE="us-central1-a"
BUCKET_NAME="san-hw2-cc"
DB_INSTANCE_NAME="hw5-database"
DB_NAME="request_logs"
DB_USER="webserver"
DB_PASSWORD="SecurePassword123!"  # Change this!

echo "========================================="
echo "Starting HW5 Infrastructure Setup"
echo "========================================="

# Set the project
gcloud config set project $PROJECT_ID

echo ""
echo "Step 1: Enabling the required APIs now..."
gcloud services enable \
    compute.googleapis.com \
    sqladmin.googleapis.com \
    storage-api.googleapis.com \
    pubsub.googleapis.com \
    cloudfunctions.googleapis.com

echo ""
echo "Step 2: Creating the Cloud SQL instance..."
# Check if instance exists
if gcloud sql instances describe $DB_INSTANCE_NAME --project=$PROJECT_ID 2>/dev/null; then
    echo "Cloud SQL instance already exists. Skipping creation."
else
    gcloud sql instances create $DB_INSTANCE_NAME \
        --database-version=MYSQL_8_0 \
        --tier=db-f1-micro \
        --region=$REGION \
        --root-password=$DB_PASSWORD \
        --backup-start-time=03:00 \
        --enable-bin-log \
        --project=$PROJECT_ID
    
    echo "Waiting for the instance to be ready..."
    sleep 30
fi

echo ""
echo "Step 3: Creating the database..."
gcloud sql databases create $DB_NAME \
    --instance=$DB_INSTANCE_NAME \
    --project=$PROJECT_ID 2>/dev/null || echo "Database already exists"

echo ""
echo "Step 4: Creating the database user..."
gcloud sql users create $DB_USER \
    --instance=$DB_INSTANCE_NAME \
    --password=$DB_PASSWORD \
    --project=$PROJECT_ID 2>/dev/null || echo "User already exists"

echo ""
echo "Step 5: Creating the database schema..."
# Import schema - need to upload to Cloud Storage first
gsutil cp sql/schema.sql gs://$BUCKET_NAME/sql/schema.sql
gcloud sql import sql $DB_INSTANCE_NAME \
    gs://$BUCKET_NAME/sql/schema.sql \
    --database=$DB_NAME \
    --project=$PROJECT_ID 2>/dev/null || echo "Schema may already exist"

echo ""
echo "Step 6: Creating the service accounts..."
# Service account for web server
SA_SERVER="hw5-webserver-sa"
SA_SERVER_EMAIL="${SA_SERVER}@${PROJECT_ID}.iam.gserviceaccount.com"

if gcloud iam service-accounts describe $SA_SERVER_EMAIL --project=$PROJECT_ID 2>/dev/null; then
    echo "Web server service account already exists"
else
    gcloud iam service-accounts create $SA_SERVER \
        --display-name="HW5 Web Server Service Account" \
        --project=$PROJECT_ID
fi

# Service account for reporter
SA_REPORTER="hw5-reporter-sa"
SA_REPORTER_EMAIL="${SA_REPORTER}@${PROJECT_ID}.iam.gserviceaccount.com"

if gcloud iam service-accounts describe $SA_REPORTER_EMAIL --project=$PROJECT_ID 2>/dev/null; then
    echo "Reporter service account already exists"
else
    gcloud iam service-accounts create $SA_REPORTER \
        --display-name="HW5 Reporter Service Account" \
        --project=$PROJECT_ID
fi

echo ""
echo "Step 7: Granting the IAM permissions..."
# Web server permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_SERVER_EMAIL" \
    --role="roles/cloudsql.client"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_SERVER_EMAIL" \
    --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_SERVER_EMAIL" \
    --role="roles/pubsub.publisher"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_SERVER_EMAIL" \
    --role="roles/logging.logWriter"

# Reporter permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_REPORTER_EMAIL" \
    --role="roles/pubsub.subscriber"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_REPORTER_EMAIL" \
    --role="roles/logging.logWriter"

echo ""
echo "Step 8: Creating the Pub/Sub topic and subscription..."
gcloud pubsub topics create forbidden-requests --project=$PROJECT_ID 2>/dev/null || echo "Topic already exists"
gcloud pubsub subscriptions create forbidden-requests-sub \
    --topic=forbidden-requests \
    --project=$PROJECT_ID 2>/dev/null || echo "Subscription already exists"

echo ""
echo "Step 9: Reserving the static IP for this..."
gcloud compute addresses create hw5-server-ip \
    --region=$REGION \
    --project=$PROJECT_ID 2>/dev/null || echo "IP already reserved"

SERVER_IP=$(gcloud compute addresses describe hw5-server-ip \
    --region=$REGION \
    --format="get(address)" \
    --project=$PROJECT_ID)

echo "Server IP: $SERVER_IP"

echo ""
echo "Step 10: Creating the firewall rule..."
gcloud compute firewall-rules create allow-http-8080 \
    --allow=tcp:8080 \
    --source-ranges=0.0.0.0/0 \
    --target-tags=http-server \
    --project=$PROJECT_ID 2>/dev/null || echo "Firewall rule already exists"

echo ""
echo "Step 11: Creating the VMs..."

# Get Cloud SQL connection name
DB_CONNECTION_NAME=$(gcloud sql instances describe $DB_INSTANCE_NAME \
    --format="get(connectionName)" \
    --project=$PROJECT_ID)

# Web Server VM
echo "Creating the web server VM..."
gcloud compute instances create hw5-server \
    --zone=$ZONE \
    --machine-type=e2-medium \
    --tags=http-server \
    --service-account=$SA_SERVER_EMAIL \
    --scopes=cloud-platform \
    --address=$SERVER_IP \
    --metadata=startup-script-url=gs://$BUCKET_NAME/hw5/startup.sh,service-type=server,project-id=$PROJECT_ID,db-connection=$DB_CONNECTION_NAME,db-name=$DB_NAME,db-user=$DB_USER,db-password=$DB_PASSWORD,bucket-name=$BUCKET_NAME \
    --project=$PROJECT_ID 2>/dev/null || echo "Server VM already exists"

# Reporter VM
echo "Creating the reporter VM..."
gcloud compute instances create hw5-reporter \
    --zone=$ZONE \
    --machine-type=e2-small \
    --service-account=$SA_REPORTER_EMAIL \
    --scopes=cloud-platform \
    --metadata=startup-script-url=gs://$BUCKET_NAME/hw5/startup.sh,service-type=reporter,project-id=$PROJECT_ID \
    --project=$PROJECT_ID 2>/dev/null || echo "Reporter VM already exists"

# Client VM
echo "Creating the client VM..."
gcloud compute instances create hw5-client \
    --zone=$ZONE \
    --machine-type=e2-small \
    --metadata=startup-script-url=gs://$BUCKET_NAME/hw5/startup.sh,service-type=client,server-ip=$SERVER_IP \
    --project=$PROJECT_ID 2>/dev/null || echo "Client VM already exists"

echo ""
echo "Step 12: Creating the database monitoring Cloud Function..."
# Note: We'll create this separately in the next step

echo ""
echo "========================================="
echo "Setup is finally Complete!"
echo "========================================="
echo "Server IP: $SERVER_IP"
echo "Database Connection: $DB_CONNECTION_NAME"
echo ""
echo "Next steps:"
echo "1. Wait 2-3 minutes for VMs to finish startup"
echo "2. Test the server: curl http://$SERVER_IP:8080/0.html"
echo "3. Check VM status: gcloud compute instances list"
echo ""
