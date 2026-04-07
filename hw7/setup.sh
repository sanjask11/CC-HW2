#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project 2>/dev/null)"
if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "ERROR: No active gcloud project set."
  exit 1
fi

REGION="us-west1"
ZONE="us-west1-a"
BUCKET="san-hw2-cc"

INPUT="gs://${BUCKET}/html-pages/*.html"
OUTPUT="gs://${BUCKET}/hw7-output-west1"
TEMP="gs://${BUCKET}/tmp-west1"
STAGING="gs://${BUCKET}/staging-west1"

JOB_NAME="hw7-dataflow-$(date +%s)-west1a"

echo "PROJECT_ID=${PROJECT_ID}"
echo "REGION=${REGION}"
echo "ZONE=${ZONE}"
echo "BUCKET=${BUCKET}"

echo "Enabling required APIs..."
gcloud services enable \
  dataflow.googleapis.com \
  compute.googleapis.com \
  storage.googleapis.com \
  logging.googleapis.com

echo "Installing Python dependencies..."
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt

echo "Ensuring bucket exists..."
if ! gsutil ls -b "gs://${BUCKET}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${BUCKET}" --location="${REGION}"
fi

echo "Preparing output/temp/staging prefixes..."
echo "hw7" | gsutil cp - "${OUTPUT}/.keep" >/dev/null
echo "hw7" | gsutil cp - "${TEMP}/.keep" >/dev/null
echo "hw7" | gsutil cp - "${STAGING}/.keep" >/dev/null

echo "Submitting Dataflow job..."
python3 hw7_beam.py \
  --input "${INPUT}" \
  --output "${OUTPUT}" \
  --topk 5 \
  --runner DataflowRunner \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --worker_zone "${ZONE}" \
  --machine_type e2-medium \
  --num_workers 1 \
  --max_num_workers 1 \
  --temp_location "${TEMP}" \
  --staging_location "${STAGING}" \
  --job_name "${JOB_NAME}" \
  --requirements_file requirements.txt

echo "Setup complete."