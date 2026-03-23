#!/usr/bin/env bash
set -euo pipefail

LOCK="/var/log/hw4_startup_done"
if [[ -f "$LOCK" ]]; then
  echo "Startup already ran once. Skipping."
  exit 0
fi

apt-get update -y
apt-get install -y python3 python3-venv python3-pip git ca-certificates

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

APPDIR="/opt/hw4"
rm -rf "$APPDIR"
mkdir -p "$APPDIR"
cd "$APPDIR"

git clone --depth=1 "$REPO_URL" repo
cp -r repo/hw4/* "$APPDIR/"

python3 -m venv "$APPDIR/venv"
"$APPDIR/venv/bin/pip" install --upgrade pip
"$APPDIR/venv/bin/pip" install -r "$APPDIR/requirements.txt"

cat >/etc/default/hw4-env <<EOF
PROJECT_ID=$PROJECT_ID
BUCKET=$BUCKET
PAGES_PREFIX=$PAGES_PREFIX
PORT=$PORT
TOPIC=$TOPIC
SUBSCRIPTION=$SUBSCRIPTION
EOF


PY_ENTRY=""
if [[ "$APP" == "service1" ]]; then
  PY_ENTRY="$APPDIR/service1_main.py"
elif [[ "$APP" == "service2" ]]; then
  PY_ENTRY="$APPDIR/service2_main.py"
else
  echo "Unknown APP=$APP" >&2
  exit 1
fi

cat >/etc/systemd/system/hw4.service <<EOF
[Unit]
Description=HW4 $APP
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/default/hw4-env
WorkingDirectory=$APPDIR
ExecStart=$APPDIR/venv/bin/python $PY_ENTRY
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable hw4.service
systemctl start hw4.service

touch "$LOCK"
echo "Startup complete."
