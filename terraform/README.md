# HFT Trading Platform - Terraform Deployment Guide

Complete Infrastructure-as-Code for deploying the HFT trading platform to Google Cloud Platform (GCP).

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     GCP PROJECT                                 │
│                                                                 │
│  ┌────────────────────────────────────────────────────────────┐│
│  │ VPC Network: 10.0.0.0/16                                  ││
│  │                                                            ││
│  │  ┌──────────────────────┐    ┌──────────────────────────┐││
│  │  │ Compute Engine VM    │    │ Memorystore Redis        │││
│  │  │ matching-engine      │    │ 2GB (configurable)       │││
│  │  │ n2-standard-4        │    │ 6379                     │││
│  │  │ 8080 WebSocket       │    └──────────────────────────┘││
│  │  │                      │                                ││
│  │  │ Docker container:    │    ┌──────────────────────────┐││
│  │  │ - Kafka consumer     │    │ Cloud Run                │││
│  │  │ - WebSocket server   │    │ telemetry-api            │││
│  │  │ - Order matching     │    │ (FastAPI)                │││
│  │  │                      │    │ Auto-scaled 1-10 replicas│││
│  │  └──────────────────────┘    └──────────────────────────┘││
│  │                                                            ││
│  │  ┌──────────────────────────────────────────────────────┐││
│  │  │ Cloud Storage (Static Files)                         │││
│  │  │ leaderboard-bucket: React app, assets                │││
│  │  └──────────────────────────────────────────────────────┘││
│  │                                                            ││
│  └────────────────────────────────────────────────────────────┘│
│                                                                 │
│  External Services (not in Terraform):                         │
│  - Kafka: Confluent Cloud or GKE (self-hosted)               │
│  - Kubernetes: For contestant sandbox (optional, via GKE)     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

### 1. Install Required Tools

```bash
# Terraform
brew install terraform  # macOS
# or download from https://www.terraform.io/downloads

# Google Cloud SDK
brew install google-cloud-sdk  # macOS
# or https://cloud.google.com/sdk/docs/install

# Verify installations
terraform version
gcloud --version
```

### 2. GCP Setup

```bash
# Login to GCP
gcloud auth application-default login

# (Optional) If you want to use existing GCP project, set it
gcloud config set project YOUR_EXISTING_PROJECT_ID
```

## Quick Start (5-10 minutes)

### Step 1: Create terraform.tfvars

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your configuration:

```hcl
project_id       = "trading-platform-hft-12345"  # Must be globally unique
gcp_region       = "us-central1"
environment      = "prod"
billing_account_id = "01234A-5B6C7D-89EFG0"  # Get from console.cloud.google.com/billing
```

### Step 2: Prepare Docker Images

Push your Docker images to Google Artifact Registry:

```bash
# Authenticate to Artifact Registry
gcloud auth configure-docker us-central1-docker.pkg.dev

# Build and push matching-engine
cd ../matching-engine
docker build -t us-central1-docker.pkg.dev/trading-platform-hft-12345/docker/matching-engine:latest .
docker push us-central1-docker.pkg.dev/trading-platform-hft-12345/docker/matching-engine:latest

# Build and push telemetry-api
cd ../telemetry
docker build -t us-central1-docker.pkg.dev/trading-platform-hft-12345/docker/telemetry-api:latest .
docker push us-central1-docker.pkg.dev/trading-platform-hft-12345/docker/telemetry-api:latest

# Update terraform.tfvars with the full image URIs:
# matching_engine_image = "us-central1-docker.pkg.dev/trading-platform-hft-12345/docker/matching-engine:latest"
# telemetry_api_image   = "us-central1-docker.pkg.dev/trading-platform-hft-12345/docker/telemetry-api:latest"
```

### Step 3: Initialize and Apply Terraform

```bash
# Initialize Terraform (download providers)
terraform init

# Validate configuration
terraform validate

# Plan the deployment (see what will be created)
terraform plan -out=tfplan

# Apply the plan
terraform apply tfplan
```

### Step 4: Capture Outputs

After `terraform apply` completes, you'll see outputs:

```
gcp_project_id = "trading-platform-hft-12345"
matching_engine_ip = "35.193.12.34"
matching_engine_ws_url = "ws://35.193.12.34:8080"
redis_host = "10.0.0.3"
redis_port = 6379
telemetry_api_url = "https://telemetry-api-abcdef123.us-central1.run.app"
leaderboard_bucket = "trading-platform-hft-12345-leaderboard"
```

Use these values to:
1. Set `EXCHANGE_GATEWAY_URL=ws://35.193.12.34:8080` in contestant pod specs
2. Set `REDIS_URL=redis://10.0.0.3:6379/0` in telemetry API
3. Deploy leaderboard React app to `trading-platform-hft-12345-leaderboard` bucket

## Deployment Details

### 1. Matching Engine (Compute Engine VM)

**What it does:**
- Runs the C++ matching engine in a Docker container
- Listens for WebSocket connections on port 8080
- Publishes trade events to Kafka

**Configuration:**
- Machine type: `n2-standard-4` (4 vCPUs, 16GB RAM) — adjust in `terraform.tfvars`
- Static IP: Reserved for stability
- Service account: `matching-engine-sa@...iam.gserviceaccount.com`
- Startup script: Installs Docker, pulls image, starts container

**Monitoring:**
- Logs: Visible in Google Cloud Logging (Compute Engine → matching-engine)
- Health check: Port 8080 TCP every 30 seconds
- Metrics: CPU, memory, disk auto-collected

**SSH Access:**
```bash
# Get the instance name
gcloud compute instances list --project=trading-platform-hft-12345

# SSH into the VM
gcloud compute ssh matching-engine-prod --project=trading-platform-hft-12345 --zone=us-central1-a

# Check container status
docker ps
docker logs matching-engine

# Tail logs in real-time
journalctl -u matching-engine.service -f
```

### 2. Redis (Memorystore)

**What it does:**
- In-memory cache for leaderboard scores and metadata
- Pub/Sub for real-time SSE updates to the frontend

**Configuration:**
- Tier: `basic` (no replication) for dev, `standard` for HA
- Memory: 2GB (default) — increase based on load
- Authorized network: Restricted to VPC 10.0.0.0/16
- No public IP (Compute Engine and Cloud Run access via VPC)

**Access:**
```bash
# From Compute Engine VM or Cloud Run service (within VPC)
redis-cli -h 10.x.x.x -p 6379

# From your local machine: use gcloud compute ssh tunnel
gcloud compute ssh matching-engine-prod --project=... -- -L 6379:REDIS_HOST:6379
redis-cli
```

### 3. Telemetry API (Cloud Run)

**What it does:**
- FastAPI service for leaderboard queries and SSE streaming
- Accepts contestant submissions (ZIP uploads)
- Auto-scales based on traffic (1-10 replicas)

**Configuration:**
- Service account: `telemetry-api-sa@...iam.gserviceaccount.com`
- Environment: `REDIS_URL`, `KAFKA_BROKERS`
- Timeout: 3600 seconds (allow long-lived SSE connections)
- Publicly accessible (CORS enabled for `*`)

**Access:**
```bash
# Get the Cloud Run service URL
gcloud run services list --project=trading-platform-hft-12345

# Call the API
curl https://telemetry-api-abcdef123.us-central1.run.app/api/v1/leaderboard
```

**Logs:**
```bash
gcloud run services describe telemetry-api-prod \
  --project=trading-platform-hft-12345 \
  --region=us-central1

gcloud logging read "resource.type=cloud_run_revision" \
  --project=trading-platform-hft-12345 \
  --limit=50
```

### 4. Storage Bucket (Leaderboard Static Files)

**What it does:**
- Hosts React leaderboard app (index.html, JavaScript bundles, assets)
- Served with CORS headers

**Deploy your app:**
```bash
# Build React app
cd leaderboard
npm install
npm run build

# Upload to GCS
gsutil -m cp -r dist/* gs://trading-platform-hft-12345-leaderboard/

# Enable CORS (optional, for local development)
gsutil cors set /path/to/cors.json gs://trading-platform-hft-12345-leaderboard/
```

**Access:**
```
https://storage.googleapis.com/trading-platform-hft-12345-leaderboard/index.html
# or
https://trading-platform-hft-12345-leaderboard.storage.googleapis.com/index.html
```

## Managing the Deployment

### View Current Infrastructure

```bash
# Show all resources created by this Terraform
terraform state list

# Show detailed info about a specific resource
terraform state show google_compute_instance.matching_engine

# Show all outputs
terraform output
```

### Update Configuration

```bash
# Change Redis memory from 2GB to 4GB
# Edit terraform.tfvars: redis_memory_gb = 4
terraform plan
terraform apply

# Scale matching engine to 8 vCPUs
# Edit terraform.tfvars: matching_engine_machine_type = "n2-standard-8"
terraform plan
terraform apply
```

### Destroy Everything

```bash
# WARNING: This deletes all GCP resources!
terraform destroy

# List what will be destroyed
terraform plan -destroy

# Confirm destruction
terraform destroy -auto-approve
```

## Cost Estimation

Monthly costs (approximate, us-central1):

| Service | Config | Cost/Month |
|---------|--------|-----------|
| Compute Engine VM | n2-standard-4 | ~$140 |
| Redis (Memorystore) | 2GB, basic | ~$50 |
| Cloud Run (telemetry-api) | 1-10 replicas, 512MB, 1 CPU | ~$10-20 |
| Storage | 100GB | ~$2-5 |
| Networking (egress) | 1TB | ~$100 |
| **TOTAL** | | **~$300-315/month** |

**Tips to reduce costs:**
- Use `redis_tier = "basic"` (no HA)
- Reduce `matching_engine_machine_type` to `e2-standard-4` (~$75/month)
- Use Confluent Cloud's cheaper tier or GKE self-hosted Kafka
- Host leaderboard on Netlify/Vercel instead of GCS (free or $20/month)

## Troubleshooting

### Matching Engine won't start

```bash
# SSH into the VM and check logs
gcloud compute ssh matching-engine-prod --project=trading-platform-hft-12345 --zone=us-central1-a

# Check systemd service status
sudo systemctl status matching-engine.service
sudo journalctl -u matching-engine.service -n 50

# Check Docker container
sudo docker logs matching-engine

# Check if image is accessible
sudo docker pull YOUR_IMAGE_URI
```

### Redis connection refused

```bash
# Check Redis is running
gcloud redis instances list --project=trading-platform-hft-12345

# Check firewall rules allow internal traffic
gcloud compute firewall-rules list --project=trading-platform-hft-12345

# Verify VPC is correct
gcloud redis instances describe leaderboard-redis-prod \
  --project=trading-platform-hft-12345 \
  --region=us-central1
```

### Cloud Run 502 Bad Gateway

```bash
# Check logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=telemetry-api-prod" \
  --project=trading-platform-hft-12345 \
  --limit=50

# Check if Redis is accessible from Cloud Run
# (Redis should be in same VPC; verify REDIS_URL env var)
gcloud run services describe telemetry-api-prod \
  --project=trading-platform-hft-12345 \
  --region=us-central1
```

## Advanced: Setting Up Kafka

This Terraform config assumes Kafka is external. Options:

### Option 1: Confluent Cloud (Recommended)

```bash
# Sign up at https://confluent.cloud
# Create cluster, topic "trade-events"
# Get brokers: pkc-xxxxx.us-central1.provider.confluent.cloud:9092

# Update terraform.tfvars
kafka_brokers = "pkc-xxxxx.us-central1.provider.confluent.cloud:9092"

# Set SASL credentials in matching engine startup script
# Or pass via Cloud Run env vars
```

### Option 2: GKE Self-Hosted Kafka

```bash
# Create GKE cluster (separate from matching engine)
gcloud container clusters create kafka-cluster \
  --project=trading-platform-hft-12345 \
  --region=us-central1 \
  --num-nodes=1

# Deploy Kafka via Helm or Bitnami charts
# Update terraform.tfvars with GKE Kafka service DNS
kafka_brokers = "kafka-broker.default.svc.cluster.local:9092"
```

## Next Steps

1. **Set up CI/CD**: Add Cloud Build to auto-deploy on git push
2. **Add monitoring**: Cloud Trace for latency analysis, Cloud Profiler for CPU
3. **Add TimescaleDB**: Uncomment Cloud SQL in `main.tf` (future task)
4. **Add GKE**: For contestant pod isolation (future task)
5. **Production hardening**: Restrict CORS, set up IAP, configure SSL certificates

## Support

- [Terraform GCP Provider Documentation](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
- [GCP Best Practices](https://cloud.google.com/docs/framework/best-practices)
- [Cloud Run Troubleshooting](https://cloud.google.com/run/docs/troubleshooting)
