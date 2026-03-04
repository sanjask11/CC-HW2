#!/usr/bin/env bash
set -euo pipefail

LOCK="/var/log/hw4_startup_done"
if [[ -f "$LOCK" ]]; then
  echo "Startup already ran once. Skipping."
  exit 0
fi


apt-get update -y
apt-get install -y python3 python3-venv python3-pip git


META="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
HDR="Metadata-Flavor: Google"

APP="$(curl -sfH "$HDR" "$META/APP")"
REPO_URL="$(curl -sfH "$HDR" "$META/REPO_URL")"
PROJECT_ID="$(curl -sfH "$HDR" "$META/PROJECT_ID")"
BUCKET="$(curl -sfH "$HDR" "$META/BUCKET")"
PAGES_PREFIX="$(curl -sfH "$HDR" "$META/PAGES_PREFIX" || echo "html-pages")"
PORT="$(curl -sfH "$HDR" "$META/PORT" || echo "8080")"
TOPIC="$(curl -sfH "$HDR" "$META/TOPIC" || true)"
SUBSCRIPTION_ID="$(curl -sfH "$HDR" "$META/SUBSCRIPTION_ID" || true)"
LOG_PREFIX="$(curl -sfH "$HDR" "$META/LOG_PREFIX" || echo "service2-logs")"


mkdir -p /opt/hw4
cd /opt/hw4
rm -rf repo
git clone "$REPO_URL" repo
cd repo/hw4

python3 -m venv /opt/hw4/venv
/opt/hw4/venv/bin/pip install --upgrade pip
/opt/hw4/venv/bin/pip install -r requirements.txt


UNIT="/etc/systemd/system/hw4-${APP}.service"

if [[ "$APP" == "server" ]]; then
  cat > "$UNIT" <<EOF
[Unit]
Description=HW4 Service1 (Web Server)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/hw4/repo/hw4
Environment=PROJECT_ID=${PROJECT_ID}
Environment=BUCKET=${BUCKET}
Environment=PAGES_PREFIX=${PAGES_PREFIX}
Environment=TOPIC=${TOPIC}
Environment=PORT=${PORT}
ExecStart=/opt/hw4/venv/bin/python /opt/hw4/repo/hw4/service1_main.py
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable "hw4-${APP}.service"
  systemctl start "hw4-${APP}.service"
  systemctl status "hw4-${APP}.service" --no-pager || true

elif [[ "$APP" == "service2" ]]; then
  cat > "$UNIT" <<EOF
[Unit]
Description=HW4 Service2 (Forbidden Request Processor)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/hw4/repo/hw4
Environment=PROJECT_ID=${PROJECT_ID}
Environment=SUBSCRIPTION_ID=${SUBSCRIPTION_ID}
Environment=BUCKET=${BUCKET}
Environment=LOG_PREFIX=${LOG_PREFIX}
ExecStart=/opt/hw4/venv/bin/python /opt/hw4/repo/hw4/service2_main.py
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable "hw4-${APP}.service"
  systemctl start "hw4-${APP}.service"
  systemctl status "hw4-${APP}.service" --no-pager || true

elif [[ "$APP" == "client" ]]; then
  echo "Client VM provisioned. No service started."
else
  echo "Unknown APP=$APP"
  exit 1
fi

touch "$LOCK"
echo "Startup complete."
