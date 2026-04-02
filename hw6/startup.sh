#!/usr/bin/env bash
set -euo pipefail

LOGFILE="/var/log/hw6-startup.log"
exec > >(tee -a "$LOGFILE") 2>&1

echo "=== HW6 Startup: $(date) ==="

LOCK="/var/log/hw6_startup_done"
if [[ -f "$LOCK" ]]; then
  echo "Startup already ran. Skipping."
  exit 0
fi

apt-get update -y
apt-get install -y python3 python3-venv python3-pip git curl ca-certificates

META="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
HDR="Metadata-Flavor: Google"

PROJECT_ID="$(curl -sfH "$HDR" "$META/PROJECT_ID")"
REPO_URL="$(curl -sfH "$HDR" "$META/REPO_URL")"
BUCKET="$(curl -sfH "$HDR" "$META/BUCKET")"
DB_NAME="$(curl -sfH "$HDR" "$META/DB_NAME")"
DB_USER="$(curl -sfH "$HDR" "$META/DB_USER")"
DB_PASSWORD="$(curl -sfH "$HDR" "$META/DB_PASSWORD")"
INSTANCE_CONNECTION_NAME="$(curl -sfH "$HDR" "$META/INSTANCE_CONNECTION_NAME")"

echo "PROJECT_ID=$PROJECT_ID"
echo "BUCKET=$BUCKET"
echo "INSTANCE_CONNECTION_NAME=$INSTANCE_CONNECTION_NAME"

APPDIR="/opt/hw6"
rm -rf "$APPDIR"
mkdir -p "$APPDIR"
cd "$APPDIR"

echo "Cloning repo..."
git clone --depth=1 "$REPO_URL" repo
cp -r repo/hw6/* "$APPDIR/"
echo "Files in APPDIR: $(ls $APPDIR)"

echo "Installing Python packages..."
python3 -m venv "$APPDIR/venv"
"$APPDIR/venv/bin/pip" install --upgrade pip
"$APPDIR/venv/bin/pip" install -r "$APPDIR/requirements.txt"
echo "Packages installed."

echo "Downloading Cloud SQL proxy..."
curl -o /usr/local/bin/cloud-sql-proxy \
  https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.11.4/cloud-sql-proxy.linux.amd64
chmod +x /usr/local/bin/cloud-sql-proxy

echo "Starting Cloud SQL proxy..."
nohup /usr/local/bin/cloud-sql-proxy "$INSTANCE_CONNECTION_NAME" \
  --address 127.0.0.1 --port 3306 >>/var/log/cloud-sql-proxy.log 2>&1 &
PROXY_PID=$!
echo "Proxy PID=$PROXY_PID"

echo "Waiting for Cloud SQL proxy to be ready..."
PROXY_READY="false"
for i in $(seq 1 30); do
  if "$APPDIR/venv/bin/python" -c "
import pymysql
pymysql.connect(
    host='127.0.0.1', port=3306,
    user='$DB_USER', password='$DB_PASSWORD',
    database='$DB_NAME', connect_timeout=5
).close()
" 2>/dev/null; then
    PROXY_READY="true"
    echo "Proxy ready on attempt $i"
    break
  fi
  echo "  attempt $i/30, waiting 5s..."
  sleep 5
done

if [[ "$PROXY_READY" != "true" ]]; then
  echo "ERROR: Cloud SQL proxy never became ready"
  cat /var/log/cloud-sql-proxy.log || true
  exit 1
fi

echo "Running train_models.py..."
export PROJECT_ID="$PROJECT_ID"
export GOOGLE_CLOUD_PROJECT="$PROJECT_ID"
export BUCKET="$BUCKET"
export DB_NAME="$DB_NAME"
export DB_USER="$DB_USER"
export DB_PASSWORD="$DB_PASSWORD"
export INSTANCE_CONNECTION_NAME="$INSTANCE_CONNECTION_NAME"
export DB_HOST="127.0.0.1"
export DB_PORT="3306"

"$APPDIR/venv/bin/python" "$APPDIR/train_models.py"

touch "$LOCK"
echo "=== HW6 Startup Done: $(date) ==="
