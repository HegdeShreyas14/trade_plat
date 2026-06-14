.PHONY: help init plan apply destroy validate fmt clean outputs logs

TERRAFORM := terraform
PROJECT_ID ?= trading-platform-hft
REGION ?= us-central1

help: ## Show this help message
	@echo "HFT Trading Platform - Terraform Commands"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make init              # Initialize Terraform"
	@echo "  make plan              # Preview changes"
	@echo "  make apply             # Deploy to GCP"
	@echo "  make destroy           # Destroy all resources"
	@echo "  make logs              # Tail matching engine logs"
	@echo ""

# ─────────────────────────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────────────────────────

setup: ## Initialize local Terraform environment
	@echo "Setting up Terraform..."
	@if [ ! -f terraform.tfvars ]; then \
		echo "Creating terraform.tfvars from template..."; \
		cp terraform.tfvars.example terraform.tfvars; \
		echo "⚠️  Edit terraform.tfvars with your GCP project details"; \
	fi
	@chmod +x deploy-images.sh
	@echo "✓ Setup complete"

init: ## Initialize Terraform (download providers)
	$(TERRAFORM) init

validate: ## Validate Terraform configuration
	$(TERRAFORM) validate

fmt: ## Format Terraform code
	$(TERRAFORM) fmt -recursive

fmt-check: ## Check if Terraform code is formatted
	$(TERRAFORM) fmt -recursive -check

# ─────────────────────────────────────────────────────────────────
# PLANNING & DEPLOYMENT
# ─────────────────────────────────────────────────────────────────

plan: validate ## Plan infrastructure changes (shows what will be created/modified)
	$(TERRAFORM) plan -out=tfplan

plan-destroy: validate ## Plan destruction of all resources
	$(TERRAFORM) plan -destroy

apply: ## Apply infrastructure changes (requires approval)
	@if [ ! -f tfplan ]; then \
		echo "No plan file found. Run 'make plan' first."; \
		exit 1; \
	fi
	$(TERRAFORM) apply tfplan

apply-auto: plan ## Plan and apply without manual approval (dangerous!)
	$(TERRAFORM) apply -auto-approve

destroy: ## Destroy all infrastructure (CAREFUL!)
	@echo "WARNING: This will destroy all GCP resources!"
	@echo "Press Ctrl+C to cancel, or Enter to continue..."
	@read -p "" dummy
	$(TERRAFORM) destroy

destroy-auto: ## Destroy all without confirmation (VERY DANGEROUS!)
	@echo "DESTROYING ALL RESOURCES WITHOUT CONFIRMATION"
	$(TERRAFORM) destroy -auto-approve

# ─────────────────────────────────────────────────────────────────
# STATE MANAGEMENT
# ─────────────────────────────────────────────────────────────────

state-list: ## List all managed resources
	$(TERRAFORM) state list

state-show: ## Show state of specific resource (e.g. make state-show RESOURCE=google_compute_instance.matching_engine)
	@if [ -z "$(RESOURCE)" ]; then \
		echo "Usage: make state-show RESOURCE=<resource>"; \
		echo "Example: make state-show RESOURCE=google_compute_instance.matching_engine"; \
		exit 1; \
	fi
	$(TERRAFORM) state show $(RESOURCE)

state-pull: ## Download current state from GCP
	$(TERRAFORM) state pull

state-refresh: ## Refresh state from actual GCP resources
	$(TERRAFORM) refresh

# ─────────────────────────────────────────────────────────────────
# OUTPUTS & INFO
# ─────────────────────────────────────────────────────────────────

outputs: ## Display Terraform outputs (IPs, URLs, etc.)
	$(TERRAFORM) output

output-matching-engine-ip: ## Get matching engine IP address
	@$(TERRAFORM) output -raw matching_engine_ip 2>/dev/null || echo "Run 'make apply' first"

output-ws-url: ## Get matching engine WebSocket URL
	@$(TERRAFORM) output -raw matching_engine_ws_url 2>/dev/null || echo "Run 'make apply' first"

output-api-url: ## Get telemetry API Cloud Run URL
	@$(TERRAFORM) output -raw telemetry_api_url 2>/dev/null || echo "Run 'make apply' first"

output-redis: ## Get Redis connection details
	@echo "Redis Host: $$($(TERRAFORM) output -raw redis_host 2>/dev/null || echo 'N/A')"
	@echo "Redis Port: $$($(TERRAFORM) output -raw redis_port 2>/dev/null || echo 'N/A')"

# ─────────────────────────────────────────────────────────────────
# DOCKER IMAGE MANAGEMENT
# ─────────────────────────────────────────────────────────────────

deploy-images: ## Build and push Docker images to Artifact Registry
	@if [ -z "$(PROJECT_ID)" ]; then \
		echo "Error: PROJECT_ID not set"; \
		echo "Usage: make deploy-images PROJECT_ID=trading-platform-hft-12345"; \
		exit 1; \
	fi
	./deploy-images.sh $(PROJECT_ID) $(REGION)

# ─────────────────────────────────────────────────────────────────
# MONITORING & DEBUGGING
# ─────────────────────────────────────────────────────────────────

logs: ## Tail matching engine logs in real-time
	gcloud logging read "resource.type=gce_instance AND resource.labels.instance_id=$(shell gcloud compute instances list --project=$(PROJECT_ID) --format='value(id)' | head -1)" \
		--project=$(PROJECT_ID) \
		--limit=50 \
		--follow

logs-cloud-run: ## Tail Cloud Run telemetry-api logs
	gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=telemetry-api-prod" \
		--project=$(PROJECT_ID) \
		--limit=50 \
		--follow

check-matching-engine: ## Check if matching engine is running
	@echo "Checking matching engine status..."
	@gcloud compute instances list --project=$(PROJECT_ID) --filter="name:matching-engine" --format="table(name,status,INTERNAL_IP)"

check-redis: ## Check Redis instance status
	@echo "Checking Redis status..."
	@gcloud redis instances list --project=$(PROJECT_ID) --format="table(name,state,host,port)"

check-cloud-run: ## Check Cloud Run service status
	@echo "Checking Cloud Run service status..."
	@gcloud run services list --project=$(PROJECT_ID) --format="table(SERVICE_NAME,STATUS,REGION,URL)"

health: ## Run all health checks
	@echo "=== System Health Check ==="
	@echo ""
	@echo "1. Matching Engine:"
	@make check-matching-engine
	@echo ""
	@echo "2. Redis:"
	@make check-redis
	@echo ""
	@echo "3. Cloud Run:"
	@make check-cloud-run
	@echo ""
	@echo "4. Network:"
	@gcloud compute firewall-rules list --project=$(PROJECT_ID) --filter="network:trading-vpc" --format="table(name,direction,priority)"

# ─────────────────────────────────────────────────────────────────
# SSH & ACCESS
# ─────────────────────────────────────────────────────────────────

ssh: ## SSH into matching engine VM
	@INSTANCE=$$(gcloud compute instances list --project=$(PROJECT_ID) --filter="name:matching-engine" --format='value(name)' | head -1); \
	if [ -z "$$INSTANCE" ]; then \
		echo "Matching engine instance not found"; \
		exit 1; \
	fi; \
	gcloud compute ssh $$INSTANCE --project=$(PROJECT_ID) --zone=us-central1-a

redis-cli: ## Connect to Redis via tunnel (from your local machine)
	@echo "Opening Redis tunnel..."
	@INSTANCE=$$(gcloud compute instances list --project=$(PROJECT_ID) --filter="name:matching-engine" --format='value(name)' | head -1); \
	gcloud compute ssh $$INSTANCE --project=$(PROJECT_ID) --zone=us-central1-a -- -L 6379:$$($(TERRAFORM) output -raw redis_host):6379

# ─────────────────────────────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────────────────────────────

clean: ## Clean up Terraform cache and plan files
	@echo "Cleaning up local Terraform files..."
	@rm -f tfplan tfplan.json
	@rm -f terraform.tfvars.bak terraform.tfvars.bak.bak
	@echo "✓ Cleanup complete"

clean-all: clean ## Clean up everything including .terraform directory
	@echo "Removing Terraform working directory..."
	@rm -rf .terraform
	@rm -f .terraform.lock.hcl
	@echo "✓ Full cleanup complete"
	@echo "Run 'make init' to reinitialize"

# ─────────────────────────────────────────────────────────────────
# COST ESTIMATION
# ─────────────────────────────────────────────────────────────────

estimate-cost: ## Estimate monthly costs
	@echo "=== Estimated Monthly Costs (us-central1) ==="
	@echo ""
	@echo "Machine Type: n2-standard-4"
	@echo "  - Compute: \$140/month"
	@echo "  - Networking (estimate): \$50-100/month"
	@echo ""
	@echo "Redis (Memorystore): 2GB basic"
	@echo "  - \$50/month"
	@echo ""
	@echo "Cloud Run (telemetry-api)"
	@echo "  - \$10-20/month (auto-scales 1-10 replicas)"
	@echo ""
	@echo "Cloud Storage (leaderboard)"
	@echo "  - \$2-5/month"
	@echo ""
	@echo "TOTAL: \$250-275/month"
	@echo ""
	@echo "Cost reduction tips:"
	@echo "  - Use e2-standard-4 instead of n2: -\$70/month"
	@echo "  - Reduce Redis to 1GB: -\$25/month"
	@echo "  - Host leaderboard on Netlify (free): -\$5/month"

# ─────────────────────────────────────────────────────────────────
# DEFAULT
# ─────────────────────────────────────────────────────────────────

.DEFAULT_GOAL := help
