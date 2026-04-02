#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export CLOUDSDK_CORE_DISABLE_PROMPTS=1

PROJECT_ID="primal-ivy-485619-r6"

ZONE="us-central1-a"
REGION="us-central1"
REPO_URL="https://github.com/sanjask11/CC-HW2.git"

BUCKET_NAME="san-hw2-cc"

ML_VM="hw6-ml-vm"
ML_SA="hw6-ml-sa"
ML_SA_EMAIL="${ML_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

DB_INSTANCE="hw6-db"
DB_NAME="hw6db"
DB_USER="hw6user"
DB_PASSWORD="hw6pass123"

TA_DATA_URI="gs://cs528-hw6-data/data.gz"

TMP_VENV="/tmp/hw6-setup-venv"
TMP_PROXY="/tmp/cloud-sql-proxy"
TMP_PROXY_LOG="/tmp/hw6_setup_proxy.log"

echo "Setting gcloud project..."
gcloud config set project "${PROJECT_ID}" >/dev/null

echo "Enabling APIs..."
gcloud services enable \
  compute.googleapis.com \
  storage.googleapis.com \
  sqladmin.googleapis.com \
  iam.googleapis.com \
  logging.googleapis.com >/dev/null

echo "Creating ML service account if missing..."
gcloud iam service-accounts create "${ML_SA}" \
  --display-name="HW6 ML SA" >/dev/null 2>&1 || true

echo "Applying IAM roles..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${ML_SA_EMAIL}" \
  --role="roles/cloudsql.client" >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${ML_SA_EMAIL}" \
  --role="roles/logging.logWriter" >/dev/null

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${ML_SA_EMAIL}" \
  --role="roles/storage.objectAdmin" >/dev/null

echo "Checking Cloud SQL instance..."
if ! gcloud sql instances describe "${DB_INSTANCE}" >/dev/null 2>&1; then
  echo "Creating Cloud SQL instance..."
  gcloud sql instances create "${DB_INSTANCE}" \
    --database-version=MYSQL_8_0 \
    --tier=db-g1-small \
    --region="${REGION}" \
    --storage-size=10GB \
    --storage-type=SSD \
    --availability-type=zonal \
    --backup-start-time=03:00 >/dev/null
else
  echo "Cloud SQL instance already exists."
fi

echo "Starting Cloud SQL instance..."
gcloud sql instances patch "${DB_INSTANCE}" \
  --activation-policy=ALWAYS \
  --quiet >/dev/null

echo "Waiting for Cloud SQL instance state RUNNABLE..."
for _ in $(seq 1 30); do
  DB_STATE="$(gcloud sql instances describe "${DB_INSTANCE}" --format='value(state)' 2>/dev/null || true)"
  if [[ "${DB_STATE}" == "RUNNABLE" ]]; then
    break
  fi
  sleep 10
done

DB_STATE="$(gcloud sql instances describe "${DB_INSTANCE}" --format='value(state)' 2>/dev/null || true)"
if [[ "${DB_STATE}" != "RUNNABLE" ]]; then
  echo "Cloud SQL instance did not become RUNNABLE. Current state=${DB_STATE}"
  exit 1
fi

echo "Ensuring database exists..."
gcloud sql databases create "${DB_NAME}" \
  --instance="${DB_INSTANCE}" >/dev/null 2>&1 || true

echo "Ensuring database user exists..."
gcloud sql users create "${DB_USER}" \
  --instance="${DB_INSTANCE}" \
  --password="${DB_PASSWORD}" >/dev/null 2>&1 || true

echo "Preparing local Python environment..."
rm -rf "${TMP_VENV}"
python3 -m venv "${TMP_VENV}"
"${TMP_VENV}/bin/pip" install --upgrade pip >/dev/null
"${TMP_VENV}/bin/pip" install PyMySQL >/dev/null

INSTANCE_CONNECTION_NAME="$(gcloud sql instances describe "${DB_INSTANCE}" --format='value(connectionName)')"

echo "Stopping any old local proxy on port 3306..."
pkill -f "cloud-sql-proxy.*127.0.0.1.*3306" >/dev/null 2>&1 || true
pkill -f "/tmp/cloud-sql-proxy" >/dev/null 2>&1 || true
sleep 2

echo "Starting temporary Cloud SQL proxy for schema setup..."
rm -f "${TMP_PROXY_LOG}"
curl -L -o "${TMP_PROXY}" \
  https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.11.4/cloud-sql-proxy.linux.amd64 >/dev/null 2>&1
chmod +x "${TMP_PROXY}"

nohup "${TMP_PROXY}" "${INSTANCE_CONNECTION_NAME}" --address 127.0.0.1 --port 3306 >"${TMP_PROXY_LOG}" 2>&1 &
PROXY_PID=$!

cleanup_proxy() {
  if ps -p "${PROXY_PID}" >/dev/null 2>&1; then
    kill "${PROXY_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup_proxy EXIT

echo "Waiting for proxy readiness..."
READY="false"
for _ in $(seq 1 30); do
  if DB_HOST=127.0.0.1 DB_PORT=3306 DB_NAME="${DB_NAME}" DB_USER="${DB_USER}" DB_PASSWORD="${DB_PASSWORD}" \
    "${TMP_VENV}/bin/python" - <<'PY' >/dev/null 2>&1
import os, pymysql
conn = pymysql.connect(
    host=os.environ["DB_HOST"],
    port=int(os.environ["DB_PORT"]),
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    database=os.environ["DB_NAME"],
    autocommit=True,
    connect_timeout=10,
    read_timeout=30,
    write_timeout=30,
    charset="utf8mb4",
)
with conn.cursor() as cur:
    cur.execute("SELECT 1")
conn.close()
PY
  then
    READY="true"
    break
  fi
  sleep 2
done

if [[ "${READY}" != "true" ]]; then
  echo "Cloud SQL proxy did not become ready."
  cat "${TMP_PROXY_LOG}" || true
  exit 1
fi

echo "Checking whether TA dataset is already imported..."
TABLE_COUNT="$(
DB_HOST=127.0.0.1 DB_PORT=3306 DB_NAME="${DB_NAME}" DB_USER="${DB_USER}" DB_PASSWORD="${DB_PASSWORD}" \
"${TMP_VENV}/bin/python" - <<'PY'
import os, pymysql
conn = pymysql.connect(
    host=os.environ["DB_HOST"],
    port=int(os.environ["DB_PORT"]),
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    database=os.environ["DB_NAME"],
    autocommit=True,
    connect_timeout=10,
    read_timeout=30,
    write_timeout=30,
    charset="utf8mb4",
)
with conn.cursor() as cur:
    cur.execute("SHOW TABLES")
    rows = cur.fetchall()
print(len(rows))
conn.close()
PY
)"

if [[ "${TABLE_COUNT}" -eq 0 ]]; then
  echo "Importing TA dataset into Cloud SQL..."
  gcloud sql import sql "${DB_INSTANCE}" "${TA_DATA_URI}" \
    --database="${DB_NAME}" \
    --project="${PROJECT_ID}" \
    --quiet >/dev/null

  echo "Waiting for imported tables to become visible..."
  IMPORT_READY="false"
  for _ in $(seq 1 30); do
    TABLE_COUNT="$(
    DB_HOST=127.0.0.1 DB_PORT=3306 DB_NAME="${DB_NAME}" DB_USER="${DB_USER}" DB_PASSWORD="${DB_PASSWORD}" \
    "${TMP_VENV}/bin/python" - <<'PY'
import os, pymysql
conn = pymysql.connect(
    host=os.environ["DB_HOST"],
    port=int(os.environ["DB_PORT"]),
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    database=os.environ["DB_NAME"],
    autocommit=True,
    connect_timeout=10,
    read_timeout=30,
    write_timeout=30,
    charset="utf8mb4",
)
with conn.cursor() as cur:
    cur.execute("SHOW TABLES")
    rows = cur.fetchall()
print(len(rows))
conn.close()
PY
)"
    if [[ "${TABLE_COUNT}" -gt 0 ]]; then
      IMPORT_READY="true"
      break
    fi
    sleep 5
  done

  if [[ "${IMPORT_READY}" != "true" ]]; then
    echo "Import completed but no tables were found."
    exit 1
  fi
else
  echo "TA dataset already present; skipping import."
fi

echo "Running setup_schema.py..."
PROJECT_ID="${PROJECT_ID}" \
DB_HOST="127.0.0.1" \
DB_PORT="3306" \
DB_NAME="${DB_NAME}" \
DB_USER="${DB_USER}" \
DB_PASSWORD="${DB_PASSWORD}" \
"${TMP_VENV}/bin/python" setup_schema.py

echo "Deleting existing ML VM if present..."
gcloud compute instances delete "${ML_VM}" \
  --zone="${ZONE}" \
  --quiet >/dev/null 2>&1 || true

echo "Creating ML VM (trying multiple zones if needed)..."
VM_CREATED="false"
for TRY_ZONE in us-central1-a us-central1-b us-central1-c us-central1-f \
                us-east1-b us-east1-c us-east1-d \
                us-east4-b us-east4-c \
                us-west1-b us-west1-c \
                us-west2-a us-west2-b \
                us-west4-a us-west4-b \
                europe-west1-b europe-west1-c \
                asia-east1-a asia-east1-b; do
  echo "  Trying zone: ${TRY_ZONE}..."
  if gcloud compute instances create "${ML_VM}" \
    --zone="${TRY_ZONE}" \
    --machine-type="e2-small" \
    --image-family="debian-12" \
    --image-project="debian-cloud" \
    --service-account="${ML_SA_EMAIL}" \
    --scopes="https://www.googleapis.com/auth/cloud-platform" \
    --metadata="PROJECT_ID=${PROJECT_ID},REPO_URL=${REPO_URL},BUCKET=${BUCKET_NAME},DB_NAME=${DB_NAME},DB_USER=${DB_USER},DB_PASSWORD=${DB_PASSWORD},INSTANCE_CONNECTION_NAME=${INSTANCE_CONNECTION_NAME}" \
    --metadata-from-file startup-script="startup.sh" \
    >/dev/null 2>&1; then
    ZONE="${TRY_ZONE}"
    VM_CREATED="true"
    echo "  VM created in zone: ${ZONE}"
    break
  fi
  echo "  Zone ${TRY_ZONE} exhausted, trying next..."
done


if [[ "${VM_CREATED}" != "true" ]]; then
  echo "ERROR: Could not create VM in any zone. Try again later."
  exit 1
fi

echo "ML VM created: ${ML_VM} in zone: ${ZONE}"
echo ""
echo "Waiting for training to complete (polls GCS every 30s, up to 30 min)..."

COUNTRY_BLOB="hw6/country_predictions.txt"
INCOME_BLOB="hw6/income_predictions.txt"
FOUND="false"

for _ in $(seq 1 60); do
  if gcloud storage cat "gs://${BUCKET_NAME}/${COUNTRY_BLOB}" >/dev/null 2>&1 && \
     gcloud storage cat "gs://${BUCKET_NAME}/${INCOME_BLOB}" >/dev/null 2>&1; then
    FOUND="true"
    break
  fi
  sleep 30
done

if [[ "${FOUND}" != "true" ]]; then
  echo "Training timed out. Fetching VM startup log for debugging..."
  gcloud compute ssh "${ML_VM}" --zone="${ZONE}" --command \
    "sudo cat /var/log/hw6-startup.log 2>/dev/null || echo 'no startup log found'" \
    2>/dev/null || echo "(Could not SSH into VM)"
fi

echo "===== Country Prediction Results ====="
gcloud storage cat "gs://${BUCKET_NAME}/${COUNTRY_BLOB}" || echo "(file not found)"

echo ""
echo "===== Income Prediction Results ====="
gcloud storage cat "gs://${BUCKET_NAME}/${INCOME_BLOB}" || echo "(file not found)"

echo ""
echo "Deleting ML VM (as required by assignment)..."
gcloud compute instances delete "${ML_VM}" \
  --zone="${ZONE}" \
  --quiet >/dev/null 2>&1 || true

echo "Stopping Cloud SQL instance (as required by assignment)..."
gcloud sql instances patch "${DB_INSTANCE}" \
  --activation-policy=NEVER \
  --quiet >/dev/null

echo "DONE"
