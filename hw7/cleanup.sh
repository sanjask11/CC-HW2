#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="$(gcloud config get-value project 2>/dev/null)"
BUCKET="san-hw2-cc"

REGIONS=("us-central1" "us-east1" "us-east4" "us-west1" "us-west2" "us-west3")

echo "PROJECT_ID=${PROJECT_ID}"
echo "Cleaning HW7 resources..."

for REGION in "${REGIONS[@]}"; do
  echo "Checking active Dataflow jobs in ${REGION}..."

  JOB_IDS=$(gcloud dataflow jobs list \
    --region="${REGION}" \
    --status=active \
    --format="value(id)" 2>/dev/null || true)

  for JOB in ${JOB_IDS}; do
    NAME=$(gcloud dataflow jobs describe "${JOB}" --region="${REGION}" --format="value(name)" 2>/dev/null || true)
    if [[ "${NAME}" == hw7-dataflow-* ]]; then
      echo "Cancelling Dataflow job: ${JOB} (${NAME}) in ${REGION}"
      gcloud dataflow jobs cancel "${JOB}" --region="${REGION}" || true
    fi
  done
done

echo "Removing HW7 output/temp/staging artifacts from bucket..."
gsutil -m rm -r "gs://${BUCKET}/hw7-output/**" 2>/dev/null || true
gsutil -m rm -r "gs://${BUCKET}/tmp/**" 2>/dev/null || true
gsutil -m rm -r "gs://${BUCKET}/staging/**" 2>/dev/null || true

gsutil -m rm -r "gs://${BUCKET}/hw7-output-east1/**" 2>/dev/null || true
gsutil -m rm -r "gs://${BUCKET}/tmp-east1/**" 2>/dev/null || true
gsutil -m rm -r "gs://${BUCKET}/staging-east1/**" 2>/dev/null || true

gsutil -m rm -r "gs://${BUCKET}/hw7-output-east4/**" 2>/dev/null || true
gsutil -m rm -r "gs://${BUCKET}/tmp-east4/**" 2>/dev/null || true
gsutil -m rm -r "gs://${BUCKET}/staging-east4/**" 2>/dev/null || true

gsutil -m rm -r "gs://${BUCKET}/hw7-output-west1/**" 2>/dev/null || true
gsutil -m rm -r "gs://${BUCKET}/tmp-west1/**" 2>/dev/null || true
gsutil -m rm -r "gs://${BUCKET}/staging-west1/**" 2>/dev/null || true

gsutil -m rm -r "gs://${BUCKET}/hw7-output-west2/**" 2>/dev/null || true
gsutil -m rm -r "gs://${BUCKET}/tmp-west2/**" 2>/dev/null || true
gsutil -m rm -r "gs://${BUCKET}/staging-west2/**" 2>/dev/null || true

gsutil -m rm -r "gs://${BUCKET}/hw7-output-west3/**" 2>/dev/null || true
gsutil -m rm -r "gs://${BUCKET}/tmp-west3/**" 2>/dev/null || true
gsutil -m rm -r "gs://${BUCKET}/staging-west3/**" 2>/dev/null || true

echo "Removing local output directory..."
rm -rf ./hw7_local_output || true

echo "Revoking application default credentials..."
gcloud auth application-default revoke || true

echo "Cleanup complete."