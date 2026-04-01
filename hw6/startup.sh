#!/usr/bin/env bash
set -euo pipefail

LOCK="/var/log/hw6_startup_done"
if [[ -f "$LOCK" ]]; then
  echo "Startup already ran once. Skipping."
  exit 0
fi

apt-get update -y
apt-get install -y python3 python3-venv python3-pip git curl ca-certificatesExecStart=$APPDIR/venv/bin/python $APPDIR/train_models.py

META="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
HDR="Metadata-Flavor: Google"

PROJECT_ID="$(curl -sfH "$HDR" "$META/PROJECT_ID")"
REPO_URL="$(curl -sfH "$HDR" "$META/REPO_URL")"
BUCKET="$(curl -sfH "$HDR" "$META/BUCKET")"
DB_NAME="$(curl -sfH "$HDR" "$META/DB_NAME")"
DB_USER="$(curl -sfH "$HDR" "$META/DB_USER")"
DB_PASSWORD="$(curl -sfH "$HDR" "$META/DB_PASSWORD")"
INSTANCE_CONNECTION_NAME="$(curl -sfH "$HDR" "$META/INSTANCE_CONNECTION_NAME")"

APPDIR="/opt/hw6"
rm -rf "$APPDIR"
mkdir -p "$APPDIR"
cd "$APPDIR"

git clone --depth=1 "$REPO_URL" repo
cp -r repo/hw6/* "$APPDIR/"

python3 -m venv "$APPDIR/venv"
"$APPDIR/venv/bin/pip" install --upgrade pip
"$APPDIR/venv/bin/pip" install -r "$APPDIR/requirements.txt"

cat >/etc/default/hw6-env <<EOF
PROJECT_ID=$PROJECT_ID
GOOGLE_CLOUD_PROJECT=$PROJECT_ID
BUCKET=$BUCKET
DB_NAME=$DB_NAME
DB_USER=$DB_USER
DB_PASSWORD=$DB_PASSWORD
INSTANCE_CONNECTION_NAME=$INSTANCE_CONNECTION_NAME
DB_HOST=127.0.0.1
DB_PORT=3306
EOF

curl -o /usr/local/bin/cloud-sql-proxy \
  https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.11.4/cloud-sql-proxy.linux.amd64
chmod +x /usr/local/bin/cloud-sql-proxy

cat >/etc/systemd/system/cloud-sql-proxy.service <<EOF
[Unit]
Description=Cloud SQL Proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/default/hw6-env
ExecStart=/usr/local/bin/cloud-sql-proxy \$INSTANCE_CONNECTION_NAME --address 127.0.0.1 --port 3306
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/hw6-train.service <<EOF
[Unit]
Description=HW6 Model Training
After=network-online.target cloud-sql-proxy.service
Wants=network-online.target
Requires=cloud-sql-proxy.service

[Service]
Type=oneshot
EnvironmentFile=/etc/default/hw6-env
WorkingDirectory=$APPDIR
ExecStart=$APPDIR/venv/bin/python $APPDIR/train_models.py
StandardOutput=append:/var/log/hw6-train.log
StandardError=append:/var/log/hw6-train.log

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable cloud-sql-proxy.service
systemctl start cloud-sql-proxy.service

sleep 8

systemctl enable hw6-train.service
systemctl start hw6-train.service

touch "$LOCK"
echo "Startup complete."