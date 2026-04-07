#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project 2>/dev/null)"
if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "ERROR: No active gcloud project set."
  exit 1
fi

REGION="us-central1"
BUCKET="san-hw2-cc"

INPUT_PATH="gs://${BUCKET}/html-pages/*.html"
OUTPUT_PREFIX="gs://${BUCKET}/hw7-output"
TEMP_PREFIX="gs://${BUCKET}/tmp"
STAGING_PREFIX="gs://${BUCKET}/staging"

echo "PROJECT_ID=${PROJECT_ID}"
echo "REGION=${REGION}"
echo "BUCKET=${BUCKET}"

echo "Enabling required APIs..."
gcloud services enable \
  dataflow.googleapis.com \
  storage.googleapis.com \
  compute.googleapis.com \
  logging.googleapis.com \
  cloudbuild.googleapis.com

echo "Creating bucket if needed..."
if ! gsutil ls -b "gs://${BUCKET}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${BUCKET}" --location="${REGION}"
fi

echo "Creating output/temp/staging prefixes..."
echo "hw7" | gsutil cp - "${OUTPUT_PREFIX}/.keep"
echo "hw7" | gsutil cp - "${TEMP_PREFIX}/.keep"
echo "hw7" | gsutil cp - "${STAGING_PREFIX}/.keep"

echo "Setting up Python environment..."
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt

echo "Running Beam job locally for validation..."
python3 hw7_beam.py \
  --input "${INPUT_PATH}" \
  --output "./hw7_local_output" \
  --topk 5 \
  --runner DirectRunner

echo "Setup complete."