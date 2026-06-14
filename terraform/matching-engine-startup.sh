#!/bin/bash
# Startup script for Matching Engine VM on Compute Engine

set -euo pipefail

# Logging
exec 1> >(logger -s -t matching-engine-startup)
exec 2>&1

echo "Starting matching engine initialization..."

# ─────────────────────────────────────────────────────────────────
# UPDATE SYSTEM
# ─────────────────────────────────────────────────────────────────

apt-get update
apt-get install -y \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg \
  lsb-release \
  vim \
  htop

# ─────────────────────────────────────────────────────────────────
# INSTALL DOCKER
# ─────────────────────────────────────────────────────────────────

curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Start and enable Docker
systemctl start docker
systemctl enable docker

# ─────────────────────────────────────────────────────────────────
# CONFIGURE DOCKER FOR GCP (AUTHENTICATION)
# ─────────────────────────────────────────────────────────────────

# Configure Docker to pull from GCR using the VM's service account
gcloud auth configure-docker gcr.io --quiet

# ─────────────────────────────────────────────────────────────────
# SYSTEM TUNING FOR LOW-LATENCY TRADING
# ─────────────────────────────────────────────────────────────────

# Increase TCP buffer sizes for high-throughput connections
sysctl -w net.core.rmem_max=134217728
sysctl -w net.core.wmem_max=134217728
sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"

# Increase max connections
sysctl -w net.core.somaxconn=65536
sysctl -w net.ipv4.tcp_max_syn_backlog=65536

# Disable Nagle's algorithm for low-latency (Docker container will also set TCP_NODELAY)
sysctl -w net.ipv4.tcp_nodelay=1

# ─────────────────────────────────────────────────────────────────
# CREATE DOCKER CONTAINER FOR MATCHING ENGINE
# ─────────────────────────────────────────────────────────────────

cat > /root/matching-engine.service << 'EOF'
[Unit]
Description=Matching Engine WebSocket Server
After=docker.service
Requires=docker.service

[Service]
Type=simple
Restart=always
RestartSec=10
User=root

ExecStart=/usr/bin/docker run \
  --name matching-engine \
  --rm \
  --network host \
  --cpus=3.5 \
  --memory=8g \
  --cap-drop=ALL \
  --cap-add=NET_BIND_SERVICE \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=512m \
  -e KAFKA_BROKERS=${kafka_brokers} \
  -e KAFKA_TOPIC=${kafka_topic} \
  -e LOG_LEVEL=info \
  ${docker_image}

ExecStop=/usr/bin/docker stop -t 5 matching-engine || true

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=matching-engine

[Install]
WantedBy=multi-user.target
EOF

# Install the service
mv /root/matching-engine.service /etc/systemd/system/
chmod 644 /etc/systemd/system/matching-engine.service
systemctl daemon-reload

# ─────────────────────────────────────────────────────────────────
# HEALTH CHECK SERVICE
# ─────────────────────────────────────────────────────────────────

cat > /root/health-check.sh << 'EOF'
#!/bin/bash
# Health check for GCP Cloud Monitoring

MATCHING_ENGINE_PORT=8080
MAX_RETRIES=3
RETRY_DELAY=2

check_health() {
  for i in $(seq 1 $MAX_RETRIES); do
    if nc -zv localhost $MATCHING_ENGINE_PORT &>/dev/null; then
      echo "Health check PASSED"
      exit 0
    fi
    if [ $i -lt $MAX_RETRIES ]; then
      sleep $RETRY_DELAY
    fi
  done

  echo "Health check FAILED - matching engine not responding on port $MATCHING_ENGINE_PORT"
  exit 1
}

check_health
EOF

chmod +x /root/health-check.sh

cat > /etc/systemd/system/health-check.timer << 'EOF'
[Unit]
Description=Matching Engine Health Check Timer
Requires=health-check.service

[Timer]
OnBootSec=30s
OnUnitActiveSec=30s
Persistent=true

[Install]
WantedBy=timers.target
EOF

cat > /etc/systemd/system/health-check.service << 'EOF'
[Unit]
Description=Matching Engine Health Check
After=matching-engine.service

[Service]
Type=oneshot
ExecStart=/root/health-check.sh
StandardOutput=journal
StandardError=journal
SyslogIdentifier=health-check

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable health-check.timer

# ─────────────────────────────────────────────────────────────────
# MONITORING & LOGGING
# ─────────────────────────────────────────────────────────────────

# Enable Google Cloud Logging (the VM service account must have logging.logWriter role)
cat > /etc/google-cloud-ops-agent/config.yaml << 'EOF'
logging:
  receivers:
    syslog:
      type: files
      include_paths:
      - /var/log/syslog
      - /var/log/messages
    docker:
      type: files
      include_paths:
      - /var/lib/docker/containers/*/*.log
  service:
    pipelines:
      default_pipeline:
        receivers: [syslog, docker]

metrics:
  receivers:
    hostmetrics:
      type: hostmetrics
      collection_interval: 60s
  service:
    pipelines:
      default_pipeline:
        receivers: [hostmetrics]
EOF

apt-get install -y google-cloud-ops-agent
systemctl restart google-cloud-ops-agent

# ─────────────────────────────────────────────────────────────────
# START MATCHING ENGINE
# ─────────────────────────────────────────────────────────────────

echo "Pulling Docker image: ${docker_image}"
docker pull ${docker_image} || {
  echo "FAILED to pull image. Check Artifact Registry configuration."
  exit 1
}

echo "Starting matching engine service..."
systemctl enable matching-engine
systemctl start matching-engine

sleep 5

# Verify container is running
if docker ps | grep -q matching-engine; then
  echo "✓ Matching engine container is running"
else
  echo "✗ Matching engine container failed to start"
  docker logs matching-engine || true
  exit 1
fi

echo "Matching engine startup complete!"
