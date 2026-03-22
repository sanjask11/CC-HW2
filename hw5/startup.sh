#!/bin/bash

set -e

echo "============================================"
echo "Starting VM initialization..."
echo "============================================"


SERVICE_TYPE=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/service-type" -H "Metadata-Flavor: Google")
PROJECT_ID=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/project-id" -H "Metadata-Flavor: Google")

echo "Service Type: $SERVICE_TYPE"
echo "Project ID: $PROJECT_ID"


echo "Updating system packages..."
apt-get update
apt-get install -y python3-pip git


echo "Installing Python dependencies..."
pip3 install --upgrade pip
pip3 install google-cloud-storage google-cloud-pubsub google-cloud-logging pymysql cryptography


echo "Cloning GitHub repository..."
cd /home
rm -rf CC-HW2
git clone https://github.com/sanjask11/CC-HW2.git
cd CC-HW2/hw5


if [ "$SERVICE_TYPE" == "server" ]; then
    echo "Setting up WEB SERVER..."
    
    # Get database metadata
    DB_CONNECTION=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/db-connection" -H "Metadata-Flavor: Google")
    DB_NAME=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/db-name" -H "Metadata-Flavor: Google")
    DB_USER=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/db-user" -H "Metadata-Flavor: Google")
    DB_PASSWORD=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/db-password" -H "Metadata-Flavor: Google")
    BUCKET_NAME=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/bucket-name" -H "Metadata-Flavor: Google")
    
    # Download and install cloud_sql_proxy
    echo "Installing Cloud SQL Proxy..."
    wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O /usr/local/bin/cloud_sql_proxy
    chmod +x /usr/local/bin/cloud_sql_proxy
    
    # Create Cloud SQL Proxy service
    cat > /etc/systemd/system/cloud-sql-proxy.service << EOF
[Unit]
Description=Cloud SQL Proxy
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/cloud_sql_proxy -instances=$DB_CONNECTION=tcp:3306
Restart=always

[Install]
WantedBy=multi-user.target
EOF
    
    # Start Cloud SQL Proxy
    systemctl daemon-reload
    systemctl enable cloud-sql-proxy
    systemctl start cloud-sql-proxy
    
    # Wait for proxy to be ready
    sleep 10
    
    # Create systemd service for web server
    cat > /etc/systemd/system/hw5.service << EOF
[Unit]
Description=HW5 Web Server
After=network.target cloud-sql-proxy.service

[Service]
Type=simple
User=root
WorkingDirectory=/home/CC-HW2/hw5
Environment="DB_HOST=127.0.0.1"
Environment="DB_PORT=3306"
Environment="DB_NAME=$DB_NAME"
Environment="DB_USER=$DB_USER"
Environment="DB_PASSWORD=$DB_PASSWORD"
Environment="PROJECT_ID=$PROJECT_ID"
Environment="BUCKET_NAME=$BUCKET_NAME"
ExecStart=/usr/bin/python3 main_service1.py
Restart=always

[Install]
WantedBy=multi-user.target
EOF
    
    systemctl daemon-reload
    systemctl enable hw5.service
    systemctl start hw5.service
    
    echo "Web server started successfully!"

elif [ "$SERVICE_TYPE" == "reporter" ]; then
    echo "Setting up REPORTER SERVICE..."
    
    # Create systemd service for reporter
    cat > /etc/systemd/system/hw5.service << EOF
[Unit]
Description=HW5 Reporter Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/home/CC-HW2/hw5
Environment="PROJECT_ID=$PROJECT_ID"
ExecStart=/usr/bin/python3 main_service2.py
Restart=always

[Install]
WantedBy=multi-user.target
EOF
    
    systemctl daemon-reload
    systemctl enable hw5.service
    systemctl start hw5.service
    
    echo "Reporter service started successfully!"

elif [ "$SERVICE_TYPE" == "client" ]; then
    echo "Setting up CLIENT VM..."
    
    SERVER_IP=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/server-ip" -H "Metadata-Flavor: Google")
    
    
    echo "Downloading http-client..."
    wget https://cs-528.bu.edu/resources/http-client/http-client-linux -O /usr/local/bin/http-client
    chmod +x /usr/local/bin/http-client
    
    echo "Client VM ready!"
    echo "Server IP: $SERVER_IP"
    echo "To run client: /usr/local/bin/http-client -d $SERVER_IP -p 8080 -n 100 -r 100"

else
    echo "Unknown service type: $SERVICE_TYPE"
    exit 1
fi

echo "============================================"
echo "VM initialization complete!"
echo "============================================"
