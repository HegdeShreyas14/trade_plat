# Quick Start: Deploy to GCP in 15 Minutes

This guide gets your trading platform live on GCP as quickly as possible.

## Prerequisites (5 min)

```bash
# 1. Install Terraform
brew install terraform  # macOS
# or download from https://www.terraform.io/downloads

# 2. Install Google Cloud SDK
brew install google-cloud-sdk  # macOS
# or https://cloud.google.com/sdk/docs/install

# 3. Login to GCP
gcloud auth application-default login

# 4. Verify installations
terraform version
gcloud --version
```

## Setup (10 min)

### Step 1: Configure GCP Project

```bash
cd terraform

# Copy example config
cp terraform.tfvars.example terraform.tfvars

# Edit terraform.tfvars - THESE MUST CHANGE:
#   project_id = "trading-platform-hft-XXXXX"  (choose a unique ID)
#   billing_account_id = "01A2B3-C4D5E6-F7G8H9"  (from console.cloud.google.com/billing)
#   gcp_region = "us-central1"  (or your preferred region)

# Quick edit if you're on macOS
open terraform.tfvars
```

**Finding your Billing Account ID:**
1. Go to https://console.cloud.google.com/billing/accounts
2. Click on your billing account
3. Copy the Account ID (format: `01A2B3-C4D5E6-F7G8H9`)

### Step 2: Build & Push Docker Images

```bash
# Make script executable
chmod +x deploy-images.sh

# Push images to Google Artifact Registry
# (This requires Docker to be running)
./deploy-images.sh trading-platform-hft-XXXXX us-central1
```

This script will:
- Create Artifact Registry repository
- Build and push matching-engine image
- Build and push telemetry-api image
- Auto-update terraform.tfvars

## Deploy (5 min)

```bash
# Initialize Terraform
terraform init

# Review what will be created
terraform plan

# Deploy!
terraform apply

# Sit back and wait ~2-3 minutes for resources to provision
```

## Get Access URLs

After `terraform apply` completes, Terraform will print:

```
Outputs:

gcp_project_id = "trading-platform-hft-12345"
matching_engine_ip = "35.193.12.34"
matching_engine_ws_url = "ws://35.193.12.34:8080"
redis_host = "10.0.0.3"
redis_port = 6379
telemetry_api_url = "https://telemetry-api-abcd1234.us-central1.run.app"
leaderboard_bucket = "trading-platform-hft-12345-leaderboard"
```

**Save these! You need them for:**

### 1. Set Contestant Pod Environment

In your Kubernetes pod spec (or orchestrator.py):
```yaml
env:
  - name: EXCHANGE_GATEWAY_URL
    value: ws://35.193.12.34:8080  # ← From matching_engine_ws_url
```

### 2. Deploy Leaderboard React App

```bash
cd leaderboard
npm install
npm run build

# Upload to Google Cloud Storage
gsutil -m cp -r dist/* gs://trading-platform-hft-12345-leaderboard/

# Access at:
# https://storage.googleapis.com/trading-platform-hft-12345-leaderboard/
```

### 3. Test the API

```bash
# Get leaderboard (empty initially)
curl https://telemetry-api-abcd1234.us-central1.run.app/api/v1/leaderboard

# Response should be []
```

## Verify Everything Works

### 1. Check Matching Engine is Running

```bash
# SSH into the VM
gcloud compute ssh matching-engine-prod \
  --project=trading-platform-hft-12345 \
  --zone=us-central1-a

# Check if Docker container is running
sudo docker ps

# Check logs
sudo journalctl -u matching-engine.service -f
```

### 2. Check Redis Connection

```bash
# From the matching engine VM SSH session:
nc -zv 10.0.0.3 6379
# Should print: Connection to 10.0.0.3 6379 port [tcp/*] succeeded!
```

### 3. Check Cloud Run Service

```bash
# Get Cloud Run service status
gcloud run services describe telemetry-api-prod \
  --project=trading-platform-hft-12345 \
  --region=us-central1

# Check logs
gcloud logging read "resource.type=cloud_run_revision" \
  --project=trading-platform-hft-12345 \
  --limit=20
```

## Testing End-to-End

### 1. Start a Test Contestant Bot

Modify your bot to connect to the cloud matching engine:

```python
# In sandbox_sdk.py or your bot code
import os
exchange_url = os.getenv("EXCHANGE_GATEWAY_URL", "ws://35.193.12.34:8080")
```

Deploy to Kubernetes with:
```yaml
env:
  - name: EXCHANGE_GATEWAY_URL
    value: ws://35.193.12.34:8080
```

### 2. Monitor Leaderboard

```bash
# Watch live updates
curl -N https://telemetry-api-abcd1234.us-central1.run.app/api/v1/leaderboard/stream

# Or open in browser to see the React UI
# https://storage.googleapis.com/trading-platform-hft-12345-leaderboard/
```

### 3. Check Kafka Flow

Verify trade events are flowing:

```bash
# If using Confluent Cloud:
confluent kafka topic produce trade-events --cloud

# If self-hosted Kafka:
kafka-console-consumer --bootstrap-server localhost:9092 --topic trade-events --from-beginning
```

## Costs (Per Month)

| Service | Cost |
|---------|------|
| Compute Engine (n2-standard-4) | ~$140 |
| Redis (2GB basic) | ~$50 |
| Cloud Run | ~$10-20 |
| Storage & Networking | ~$10-20 |
| **TOTAL** | **~$210-230** |

To reduce costs:
- Use `e2-standard-4` VM (~$75/month) instead of n2
- Reduce Redis memory to 1GB (~$25/month)
- Host leaderboard on Netlify/Vercel (free)

## Cleanup

```bash
# Destroy all GCP resources (careful!)
terraform destroy

# List what will be deleted
terraform plan -destroy
```

## Common Issues

### "Error acquiring the state lock"
```bash
# Clean up Terraform lock
rm -rf .terraform/.lock.hcl
terraform destroy
```

### "Image not found" when matching-engine starts
```bash
# Verify image was pushed
gcloud artifacts docker images list us-central1-docker.pkg.dev/trading-platform-hft-12345/docker/

# Re-push if needed
./deploy-images.sh trading-platform-hft-12345 us-central1
```

### "Cannot connect to Redis"
```bash
# Verify Redis is in the correct VPC
gcloud redis instances describe leaderboard-redis-prod --region=us-central1

# Check firewall rules
gcloud compute firewall-rules list --filter="name:allow-internal"
```

### Cloud Run returns 502
```bash
# Check logs
gcloud logging read "resource.type=cloud_run_revision" --limit=50

# Verify Redis URL is correct:
gcloud run services describe telemetry-api-prod --region=us-central1
# Look for REDIS_URL env var
```

## Next Steps

1. **Upload first contestant submission** to test the full flow
2. **Set up monitoring**: Enable Cloud Logging and Cloud Trace
3. **Add Kafka** (Confluent Cloud recommended for simplicity)
4. **Add GKE** for contestant sandboxing
5. **Add TimescaleDB** for historical analytics

## Support

For issues:
1. Check **Cloud Logging**: https://console.cloud.google.com/logs
2. Check **Cloud Compute** status: https://console.cloud.google.com/compute/instances
3. Check **Cloud Run** logs: https://console.cloud.google.com/run/detail
4. Read `terraform/README.md` for detailed troubleshooting

## You're Live!

Your trading platform is now running on GCP. 🚀

- **Matching Engine**: ws://35.193.12.34:8080
- **Leaderboard API**: https://telemetry-api-abcd1234.us-central1.run.app
- **Dashboard**: https://storage.googleapis.com/trading-platform-hft-12345-leaderboard/
