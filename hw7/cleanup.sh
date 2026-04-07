#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project 2>/dev/null)"
REGION="us-central1"
BUCKET="san-hw2-cc"

echo "PROJECT_ID=${PROJECT_ID}"
echo "Cleaning HW7 resources..."


echo "Checking for running Dataflow jobs..."
JOB_IDS=$(gcloud dataflow jobs list \
  --region="${REGION}" \
  --status=active \
  --format="value(id)" || true)

for JOB in ${JOB_IDS}; do
  NAME=$(gcloud dataflow jobs describe "$JOB" --region="${REGION}" --format="value(name)")
  if [[ "$NAME" == *"hw7"* ]]; then
    echo "Cancelling Dataflow job: $JOB ($NAME)"
    gcloud dataflow jobs cancel "$JOB" --region="${REGION}"
  fi
done


echo "Removing HW7 output from bucket..."
gsutil -m rm -r "gs://${BUCKET}/hw7-output/"* || true


echo "Removing temp and staging files..."
gsutil -m rm -r "gs://${BUCKET}/tmp/"* || true
gsutil -m rm -r "gs://${BUCKET}/staging/"* || true


if [ -d "./hw7_local_output" ]; then
  echo "Removing local output directory..."
  rm -rf ./hw7_local_output
fi


echo "Revoking application default credentials..."
gcloud auth application-default revoke || true

echo "Cleanup complete."