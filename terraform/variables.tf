# ─────────────────────────────────────────────────────────────────
# GCP CONFIGURATION
# ─────────────────────────────────────────────────────────────────

variable "project_id" {
  description = "GCP Project ID (must be globally unique)"
  type        = string
  default     = "trading-platform-hft"

  validation {
    condition     = length(var.project_id) >= 6 && length(var.project_id) <= 30
    error_message = "Project ID must be 6-30 characters."
  }
}

variable "project_name" {
  description = "Friendly name for the GCP project"
  type        = string
  default     = "HFT Trading Platform"
}

variable "billing_account_id" {
  description = "GCP Billing Account ID (optional if you're creating just one project)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "gcp_region" {
  description = "Primary GCP region for deployment"
  type        = string
  default     = "us-central1"

  validation {
    condition = contains([
      "us-central1",
      "us-east1",
      "us-west1",
      "us-west2",
      "us-west3",
      "us-west4",
      "europe-west1",
      "asia-southeast1",
    ], var.gcp_region)
    error_message = "Region must be a valid GCP region."
  }
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "prod"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

# ─────────────────────────────────────────────────────────────────
# NETWORK & SECURITY
# ─────────────────────────────────────────────────────────────────

variable "allowed_ssh_cidr" {
  description = "CIDR blocks allowed to SSH into the matching engine VM"
  type        = list(string)
  default     = ["0.0.0.0/0"] # WARNING: Restrict this in production!

  validation {
    condition     = length(var.allowed_ssh_cidr) > 0
    error_message = "At least one CIDR block must be specified."
  }
}

# ─────────────────────────────────────────────────────────────────
# REDIS (Memorystore)
# ─────────────────────────────────────────────────────────────────

variable "redis_tier" {
  description = "Redis tier: basic or standard"
  type        = string
  default     = "basic"

  validation {
    condition     = contains(["basic", "standard"], var.redis_tier)
    error_message = "Redis tier must be basic or standard."
  }
}

variable "redis_memory_gb" {
  description = "Redis memory size in GB (1-300 for basic, 1-300+ for standard)"
  type        = number
  default     = 2

  validation {
    condition     = var.redis_memory_gb >= 1 && var.redis_memory_gb <= 300
    error_message = "Redis memory must be between 1 and 300 GB."
  }
}

# ─────────────────────────────────────────────────────────────────
# MATCHING ENGINE (Compute Engine VM)
# ─────────────────────────────────────────────────────────────────

variable "matching_engine_machine_type" {
  description = "Machine type for the matching engine VM (should have at least 4 CPUs for core pinning)"
  type        = string
  default     = "n2-standard-4"

  validation {
    condition = contains([
      "n2-standard-4",
      "n2-standard-8",
      "n2-standard-16",
      "c2-standard-4",
      "c2-standard-8",
      "c3-standard-4",
      "c3-standard-8",
    ], var.matching_engine_machine_type)
    error_message = "Machine type must be a valid GCP machine type with at least 4 vCPUs."
  }
}

variable "matching_engine_image" {
  description = "Docker image for the matching engine (from Artifact Registry or Docker Hub)"
  type        = string
  default     = "gcr.io/trading-platform-hft/matching-engine:latest"
}

# ─────────────────────────────────────────────────────────────────
# TELEMETRY API (Cloud Run)
# ─────────────────────────────────────────────────────────────────

variable "telemetry_api_image" {
  description = "Docker image for the telemetry API (from Artifact Registry or Docker Hub)"
  type        = string
  default     = "gcr.io/trading-platform-hft/telemetry-api:latest"
}

variable "kafka_brokers" {
  description = "Kafka broker addresses (comma-separated)"
  type        = string
  default     = "kafka:29092"
  # For Confluent Cloud, use: "pkc-xxxxx.us-central1.provider.confluent.cloud:9092"
  # Make sure to set KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD env vars if using Confluent
}

# ─────────────────────────────────────────────────────────────────
# TAGS & LABELS
# ─────────────────────────────────────────────────────────────────

variable "labels" {
  description = "Common labels to apply to all resources"
  type        = map(string)
  default = {
    project     = "trading-platform"
    environment = "production"
    managed_by  = "terraform"
  }
}
