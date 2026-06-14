#!/bin/bash
# Deploy Docker images to Google Artifact Registry
# Usage: ./deploy-images.sh <project_id> <region>

set -euo pipefail

PROJECT_ID="${1:-}"
REGION="${2:-us-central1}"
REGISTRY="${REGION}-docker.pkg.dev"

if [ -z "$PROJECT_ID" ]; then
  echo "Usage: $0 <project_id> [region]"
  echo "Example: $0 trading-platform-hft-12345 us-central1"
  exit 1
fi

REPO_NAME="docker"
MATCHING_ENGINE_IMAGE="matching-engine"
TELEMETRY_API_IMAGE="telemetry-api"

echo "==================================================================="
echo "Deploying Docker Images to Google Artifact Registry"
echo "==================================================================="
echo "Project ID: $PROJECT_ID"
echo "Region: $REGION"
echo "Registry: $REGISTRY"
echo ""

# ─────────────────────────────────────────────────────────────────
# AUTHENTICATE WITH GCP
# ─────────────────────────────────────────────────────────────────

echo "Step 1: Authenticating with GCP..."
gcloud auth configure-docker "${REGISTRY}" --quiet
echo "✓ Authentication complete"
echo ""

# ─────────────────────────────────────────────────────────────────
# CREATE ARTIFACT REGISTRY REPOSITORY (if it doesn't exist)
# ─────────────────────────────────────────────────────────────────

echo "Step 2: Creating Artifact Registry repository..."
if gcloud artifacts repositories describe "$REPO_NAME" \
  --location="$REGION" \
  --project="$PROJECT_ID" \
  &>/dev/null; then
  echo "✓ Repository '$REPO_NAME' already exists"
else
  echo "Creating repository '$REPO_NAME'..."
  gcloud artifacts repositories create "$REPO_NAME" \
    --repository-format=docker \
    --location="$REGION" \
    --project="$PROJECT_ID"
  echo "✓ Repository created"
fi
echo ""

# ─────────────────────────────────────────────────────────────────
# BUILD AND PUSH MATCHING ENGINE IMAGE
# ─────────────────────────────────────────────────────────────────

echo "Step 3: Building and pushing matching-engine image..."
MATCHING_ENGINE_URI="${REGISTRY}/${PROJECT_ID}/${REPO_NAME}/${MATCHING_ENGINE_IMAGE}:latest"

# Check if Dockerfile exists
if [ ! -f "../matching-engine/Dockerfile" ]; then
  echo "✗ Error: ../matching-engine/Dockerfile not found"
  exit 1
fi

echo "Building image: $MATCHING_ENGINE_URI"
docker build \
  -t "$MATCHING_ENGINE_URI" \
  -f ../matching-engine/Dockerfile \
  ../

echo "Pushing image to registry..."
docker push "$MATCHING_ENGINE_URI"
echo "✓ Matching engine image pushed: $MATCHING_ENGINE_URI"
echo ""

# ─────────────────────────────────────────────────────────────────
# BUILD AND PUSH TELEMETRY API IMAGE
# ─────────────────────────────────────────────────────────────────

echo "Step 4: Building and pushing telemetry-api image..."
TELEMETRY_API_URI="${REGISTRY}/${PROJECT_ID}/${REPO_NAME}/${TELEMETRY_API_IMAGE}:latest"

# Check if Dockerfile exists
if [ ! -f "../telemetry/Dockerfile" ]; then
  echo "✗ Error: ../telemetry/Dockerfile not found"
  exit 1
fi

echo "Building image: $TELEMETRY_API_URI"
docker build \
  -t "$TELEMETRY_API_URI" \
  -f ../telemetry/Dockerfile \
  ../

echo "Pushing image to registry..."
docker push "$TELEMETRY_API_URI"
echo "✓ Telemetry API image pushed: $TELEMETRY_API_URI"
echo ""

# ─────────────────────────────────────────────────────────────────
# UPDATE terraform.tfvars
# ─────────────────────────────────────────────────────────────────

echo "Step 5: Updating terraform.tfvars with image URIs..."

# Check if terraform.tfvars exists
if [ ! -f "terraform.tfvars" ]; then
  echo "✗ Error: terraform.tfvars not found. Run 'cp terraform.tfvars.example terraform.tfvars' first"
  exit 1
fi

# Backup original
cp terraform.tfvars terraform.tfvars.bak

# Update image URIs (using sed with | as delimiter to avoid escaping forward slashes)
sed -i.bak "s|matching_engine_image = .*|matching_engine_image = \"${MATCHING_ENGINE_URI}\"|" terraform.tfvars
sed -i.bak "s|telemetry_api_image = .*|telemetry_api_image = \"${TELEMETRY_API_URI}\"|" terraform.tfvars

echo "✓ Updated terraform.tfvars with new image URIs"
echo ""

# ─────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────

echo "==================================================================="
echo "✓ DEPLOYMENT COMPLETE"
echo "==================================================================="
echo ""
echo "Image URIs (now in terraform.tfvars):"
echo "  Matching Engine: $MATCHING_ENGINE_URI"
echo "  Telemetry API:   $TELEMETRY_API_URI"
echo ""
echo "Next steps:"
echo "  1. Review terraform.tfvars for any other customizations"
echo "  2. Run: terraform init"
echo "  3. Run: terraform plan"
echo "  4. Run: terraform apply"
echo ""
echo "Verify images in registry:"
echo "  gcloud artifacts docker images list ${REGISTRY}/${PROJECT_ID}/${REPO_NAME} --project=${PROJECT_ID}"
echo ""
