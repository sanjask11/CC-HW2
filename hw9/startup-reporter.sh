#!/usr/bin/env bash
set -euo pipefail

LOCK="/var/log/hw9_reporter_startup_done"
if [[ -f "$LOCK" ]]; then
  echo "Startup already ran once. Skipping."
  exit 0
fi

apt-get update -y
apt-get install -y python3 python3-venv python3-pip git

META="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
HDR="Metadata-Flavor: Google"

REPO_URL="$(curl -sfH "$HDR" "$META/REPO_URL")"
PROJECT_ID="$(curl -sfH "$HDR" "$META/PROJECT_ID")"
SUBSCRIPTION="$(curl -sfH "$HDR" "$META/SUBSCRIPTION")"

APPDIR="/opt/hw9-reporter"
rm -rf "$APPDIR"
mkdir -p "$APPDIR"
cd "$APPDIR"

git clone --depth=1 "$REPO_URL" repo
cp -r repo/hw9/* "$APPDIR/"

python3 -m venv "$APPDIR/venv"
"$APPDIR/venv/bin/pip" install --upgrade pip
"$APPDIR/venv/bin/pip" install -r "$APPDIR/requirements.txt"

cat >/etc/default/hw9-reporter-env <<EOF
PROJECT_ID=$PROJECT_ID
SUBSCRIPTION=$SUBSCRIPTION
EOF

cat >/etc/systemd/system/hw9-reporter.service <<EOF
[Unit]
Description=HW9 Reporter
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/default/hw9-reporter-env
WorkingDirectory=$APPDIR
ExecStart=$APPDIR/venv/bin/python $APPDIR/service2_main.py
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable hw9-reporter.service
systemctl start hw9-reporter.service

touch "$LOCK"
echo "Startup complete."