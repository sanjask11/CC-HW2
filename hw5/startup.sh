#!/usr/bin/env bash
set -euo pipefail

LOCK="/var/log/hw5_startup_done"
if [[ -f "$LOCK" ]]; then
  echo "Startup already ran once. Skipping."
  exit 0
fi

apt-get update -y
apt-get install -y python3 python3-venv python3-pip git ca-certificates curl default-mysql-client

META="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
HDR="Metadata-Flavor: Google"

APP="$(curl -sfH "$HDR" "$META/APP")"
REPO_URL="$(curl -sfH "$HDR" "$META/REPO_URL")"
PROJECT_ID="$(curl -sfH "$HDR" "$META/PROJECT_ID")"
BUCKET="$(curl -sfH "$HDR" "$META/BUCKET" || true)"
PAGES_PREFIX="$(curl -sfH "$HDR" "$META/PAGES_PREFIX" || echo "html-pages")"
PORT="$(curl -sfH "$HDR" "$META/PORT" || echo "8080")"
TOPIC="$(curl -sfH "$HDR" "$META/TOPIC" || true)"
SUBSCRIPTION="$(curl -sfH "$HDR" "$META/SUBSCRIPTION" || true)"
DB_HOST="$(curl -sfH "$HDR" "$META/DB_HOST" || echo "127.0.0.1")"
DB_PORT="$(curl -sfH "$HDR" "$META/DB_PORT" || echo "3306")"
DB_USER="$(curl -sfH "$HDR" "$META/DB_USER" || true)"
DB_PASSWORD="$(curl -sfH "$HDR" "$META/DB_PASSWORD" || true)"
DB_NAME="$(curl -sfH "$HDR" "$META/DB_NAME" || echo "requestsdb")"
DB_CONNECTION_NAME="$(curl -sfH "$HDR" "$META/DB_CONNECTION_NAME" || true)"

APPDIR="/opt/hw5"
rm -rf "$APPDIR"
mkdir -p "$APPDIR"
cd "$APPDIR"

git clone --depth=1 "$REPO_URL" repo
cp -r repo/hw5/* "$APPDIR/"

python3 -m venv "$APPDIR/venv"
"$APPDIR/venv/bin/pip" install --upgrade pip
"$APPDIR/venv/bin/pip" install -r "$APPDIR/requirements.txt"

cat >/etc/default/hw5-env <<EOF
PROJECT_ID=$PROJECT_ID
BUCKET=$BUCKET
PAGES_PREFIX=$PAGES_PREFIX
PORT=$PORT
TOPIC=$TOPIC
SUBSCRIPTION=$SUBSCRIPTION
DB_HOST=$DB_HOST
DB_PORT=$DB_PORT
DB_USER=$DB_USER
DB_PASSWORD=$DB_PASSWORD
DB_NAME=$DB_NAME
EOF

if [[ "$APP" == "service1" ]]; then
  curl -fsSL -o /usr/local/bin/cloud-sql-proxy \
    https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.14.3/cloud-sql-proxy.linux.amd64
  chmod +x /usr/local/bin/cloud-sql-proxy

  cat >/etc/systemd/system/cloud-sql-proxy.service <<EOF
[Unit]
Description=Cloud SQL Proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/cloud-sql-proxy --address 127.0.0.1 --port 3306 $DB_CONNECTION_NAME
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable cloud-sql-proxy.service
  systemctl start cloud-sql-proxy.service

  sleep 8

  mysql -h 127.0.0.1 -P 3306 -u "$DB_USER" -p"$DB_PASSWORD" < "$APPDIR/sql/schema.sql" || true
fi

if [[ "$APP" == "service1" ]]; then
  PY_ENTRY="$APPDIR/main_service1.py"
elif [[ "$APP" == "service2" ]]; then
  PY_ENTRY="$APPDIR/main_service2.py"
else
  echo "Unknown APP=$APP" >&2
  exit 1
fi

cat >/etc/systemd/system/hw5.service <<EOF
[Unit]
Description=HW5 $APP
After=network-online.target
Wants=network-online.target
EOF

if [[ "$APP" == "service1" ]]; then
cat >>/etc/systemd/system/hw5.service <<EOF
Requires=cloud-sql-proxy.service
After=cloud-sql-proxy.service
EOF
fi

cat >>/etc/systemd/system/hw5.service <<EOF

[Service]
Type=simple
EnvironmentFile=/etc/default/hw5-env
WorkingDirectory=$APPDIR
ExecStart=$APPDIR/venv/bin/python $PY_ENTRY
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable hw5.service
systemctl start hw5.service

touch "$LOCK"
echo "Startup complete."
