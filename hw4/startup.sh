#!/bin/bash
set -e

LOCK=/var/log/startup_already_done
if [ -f "$LOCK" ]; then
  exit 0
fi

META="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
HDR="Metadata-Flavor: Google"

APP=$(curl -s -H "$HDR" "$META/APP" || true)
REPO_URL=$(curl -s -H "$HDR" "$META/REPO_URL" || true)

BUCKET=$(curl -s -H "$HDR" "$META/BUCKET" || true)
PAGES_PREFIX=$(curl -s -H "$HDR" "$META/PAGES_PREFIX" || true)
TOPIC=$(curl -s -H "$HDR" "$META/TOPIC" || true)
SUBSCRIPTION=$(curl -s -H "$HDR" "$META/SUBSCRIPTION" || true)
PORT=$(curl -s -H "$HDR" "$META/PORT" || true)

# defaults (safe)
: "${BUCKET:=san-hw2-cc}"
: "${PAGES_PREFIX:=html-pages}"
: "${TOPIC:=forbidden-requests}"
: "${SUBSCRIPTION:=forbidden-requests-sub}"
: "${PORT:=8080}"

apt-get update
apt-get install -y python3-pip git

rm -rf /opt/repo
git clone "$REPO_URL" /opt/repo

pip3 install -r /opt/repo/hw4/requirements.txt

cat <<EOF > /etc/default/hw4
BUCKET="$BUCKET"
PAGES_PREFIX="$PAGES_PREFIX"
TOPIC="$TOPIC"
SUBSCRIPTION="$SUBSCRIPTION"
PORT="$PORT"
EOF

if [ "$APP" = "service1" ]; then
  cat <<'EOF' > /etc/systemd/system/service1.service
[Unit]
Description=HW4 Service 1 Web Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/repo/hw4
EnvironmentFile=/etc/default/hw4
ExecStart=/usr/bin/gunicorn -w 1 -b 0.0.0.0:${PORT} service1_main:app
Restart=always

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable --now service1
fi

if [ "$APP" = "service2" ]; then
  cat <<'EOF' > /etc/systemd/system/service2.service
[Unit]
Description=HW4 Service 2 Forbidden Reporter
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/repo/hw4
EnvironmentFile=/etc/default/hw4
ExecStart=/usr/bin/python3 /opt/repo/hw4/service2_main.py
Restart=always

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable --now service2
fi

touch "$LOCK"
