terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }

  # Uncomment this after first apply to use remote state
  # backend "gcs" {
  #   bucket = "YOUR_TERRAFORM_STATE_BUCKET"
  #   prefix = "trade-platform"
  # }
}

provider "google" {
  project = google_project.trading_platform.project_id
  region  = var.gcp_region
}

provider "google-beta" {
  project = google_project.trading_platform.project_id
  region  = var.gcp_region
}

# ─────────────────────────────────────────────────────────────────
# PROJECT CREATION
# ─────────────────────────────────────────────────────────────────

resource "google_project" "trading_platform" {
  name            = var.project_name
  project_id      = var.project_id
  billing_account = var.billing_account_id
  # If billing_account_id is empty, you'll need to link it manually in the console
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "compute.googleapis.com",
    "redis.googleapis.com",
    "run.googleapis.com",
    "storage.googleapis.com",
    "container.googleapis.com",
    "iam.googleapis.com",
  ])

  project = google_project.trading_platform.project_id
  service = each.value

  disable_on_destroy = false
}

# ─────────────────────────────────────────────────────────────────
# NETWORKING
# ─────────────────────────────────────────────────────────────────

resource "google_compute_network" "trading_vpc" {
  name                    = "${var.environment}-trading-vpc"
  project                 = google_project.trading_platform.project_id
  auto_create_subnetworks = false

  depends_on = [google_project_service.required_apis]
}

resource "google_compute_subnetwork" "trading_subnet" {
  name          = "${var.environment}-trading-subnet"
  ip_cidr_range = "10.0.0.0/16"
  region        = var.gcp_region
  network       = google_compute_network.trading_vpc.id
  project       = google_project.trading_platform.project_id
}

# ─────────────────────────────────────────────────────────────────
# FIREWALL RULES
# ─────────────────────────────────────────────────────────────────

# Allow internal VPC traffic
resource "google_compute_firewall" "allow_internal" {
  name    = "${var.environment}-allow-internal"
  network = google_compute_network.trading_vpc.name
  project = google_project.trading_platform.project_id

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  source_ranges = ["10.0.0.0/16"]
}

# Allow SSH from anywhere (restrict to your IP in production)
resource "google_compute_firewall" "allow_ssh" {
  name    = "${var.environment}-allow-ssh"
  network = google_compute_network.trading_vpc.name
  project = google_project.trading_platform.project_id

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = var.allowed_ssh_cidr
}

# Allow WebSocket traffic to matching engine (port 8080)
resource "google_compute_firewall" "allow_websocket" {
  name    = "${var.environment}-allow-websocket"
  network = google_compute_network.trading_vpc.name
  project = google_project.trading_platform.project_id

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["matching-engine"]
}

# Allow health checks
resource "google_compute_firewall" "allow_health_checks" {
  name    = "${var.environment}-allow-health-checks"
  network = google_compute_network.trading_vpc.name
  project = google_project.trading_platform.project_id

  allow {
    protocol = "tcp"
  }

  source_ranges = ["35.191.0.0/16", "130.211.0.0/22"]
  target_tags   = ["matching-engine"]
}

# ─────────────────────────────────────────────────────────────────
# SERVICE ACCOUNT FOR MATCHING ENGINE VM
# ─────────────────────────────────────────────────────────────────

resource "google_service_account" "matching_engine_sa" {
  account_id   = "${var.environment}-matching-engine-sa"
  display_name = "Service Account for Matching Engine"
  project      = google_project.trading_platform.project_id
}

# Allow pulling images from Google Container Registry
resource "google_project_iam_member" "matching_engine_log_writer" {
  project = google_project.trading_platform.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.matching_engine_sa.email}"
}

resource "google_project_iam_member" "matching_engine_metric_writer" {
  project = google_project.trading_platform.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.matching_engine_sa.email}"
}

# ─────────────────────────────────────────────────────────────────
# REDIS INSTANCE (Memorystore)
# ─────────────────────────────────────────────────────────────────

resource "google_redis_instance" "leaderboard_redis" {
  name               = "${var.environment}-leaderboard-redis"
  tier               = var.redis_tier
  memory_size_gb     = var.redis_memory_gb
  region             = var.gcp_region
  location_id        = "${var.gcp_region}-a"
  redis_version      = "7.0"
  authorized_network = google_compute_network.trading_vpc.id
  project            = google_project.trading_platform.project_id

  display_name = "Leaderboard Redis Cache"

  depends_on = [
    google_project_service.required_apis,
    google_compute_network.trading_vpc
  ]
}

# ─────────────────────────────────────────────────────────────────
# STORAGE BUCKET FOR LEADERBOARD STATIC FILES
# ─────────────────────────────────────────────────────────────────

resource "google_storage_bucket" "leaderboard_bucket" {
  name          = "${var.project_id}-leaderboard"
  location      = var.gcp_region
  project       = google_project.trading_platform.project_id
  force_destroy = false

  uniform_bucket_level_access = true

  website {
    main_page_suffix = "index.html"
    not_found_page   = "index.html"
  }

  depends_on = [google_project_service.required_apis]
}

# Make bucket public for static content
resource "google_storage_bucket_iam_member" "leaderboard_public" {
  bucket = google_storage_bucket.leaderboard_bucket.name
  role   = "roles/storage.objectViewer"
  member = "allUsers"
}

# ─────────────────────────────────────────────────────────────────
# CLOUD RUN SERVICE (Telemetry API)
# ─────────────────────────────────────────────────────────────────

resource "google_service_account" "telemetry_api_sa" {
  account_id   = "${var.environment}-telemetry-api-sa"
  display_name = "Service Account for Telemetry API"
  project      = google_project.trading_platform.project_id
}

# IAM: allow Cloud Run to write logs
resource "google_project_iam_member" "telemetry_api_log_writer" {
  project = google_project.trading_platform.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.telemetry_api_sa.email}"
}

resource "google_cloud_run_service" "telemetry_api" {
  name     = "${var.environment}-telemetry-api"
  location = var.gcp_region
  project  = google_project.trading_platform.project_id

  template {
    spec {
      service_account_name = google_service_account.telemetry_api_sa.email

      containers {
        image = var.telemetry_api_image

        env {
          name  = "REDIS_URL"
          value = "redis://${google_redis_instance.leaderboard_redis.host}:${google_redis_instance.leaderboard_redis.port}/0"
        }

        env {
          name  = "KAFKA_BROKERS"
          value = var.kafka_brokers
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }

      timeout_seconds = 3600 # Allow long-lived SSE connections
    }

    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale" = "10"
        "autoscaling.knative.dev/minScale" = "1"
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [google_project_service.required_apis]
}

# Make Cloud Run service public
resource "google_cloud_run_service_iam_member" "telemetry_api_public" {
  service       = google_cloud_run_service.telemetry_api.name
  location      = google_cloud_run_service.telemetry_api.location
  role          = "roles/run.invoker"
  member        = "allUsers"
  project       = google_project.trading_platform.project_id
}

# ─────────────────────────────────────────────────────────────────
# COMPUTE ENGINE VM FOR MATCHING ENGINE
# ─────────────────────────────────────────────────────────────────

resource "google_compute_instance" "matching_engine" {
  name         = "${var.environment}-matching-engine"
  machine_type = var.matching_engine_machine_type
  zone         = "${var.gcp_region}-a"
  project      = google_project.trading_platform.project_id

  tags = ["matching-engine", "http-server", "https-server"]

  boot_disk {
    initialize_params {
      image = "debian-12-amd64"
      size  = 30
      type  = "pd-standard"
    }
  }

  network_interface {
    network    = google_compute_network.trading_vpc.name
    subnetwork = google_compute_subnetwork.trading_subnet.name

    access_config {
      nat_ip = google_compute_address.matching_engine_ip.address
    }
  }

  service_account {
    email  = google_service_account.matching_engine_sa.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    enable-oslogin = "TRUE"
  }

  metadata_startup_script = base64encode(templatefile("${path.module}/matching-engine-startup.sh", {
    docker_image       = var.matching_engine_image
    kafka_brokers      = var.kafka_brokers
    kafka_topic        = "trade-events"
    gcp_region         = var.gcp_region
    project_id         = google_project.trading_platform.project_id
  }))

  depends_on = [
    google_project_service.required_apis,
    google_compute_network.trading_vpc
  ]
}

# Static IP for matching engine
resource "google_compute_address" "matching_engine_ip" {
  name    = "${var.environment}-matching-engine-ip"
  region  = var.gcp_region
  project = google_project.trading_platform.project_id
}

# ─────────────────────────────────────────────────────────────────
# OUTPUTS
# ─────────────────────────────────────────────────────────────────

output "gcp_project_id" {
  description = "GCP Project ID"
  value       = google_project.trading_platform.project_id
}

output "matching_engine_ip" {
  description = "Public IP address of the matching engine VM"
  value       = google_compute_address.matching_engine_ip.address
}

output "matching_engine_ws_url" {
  description = "WebSocket URL for the matching engine"
  value       = "ws://${google_compute_address.matching_engine_ip.address}:8080"
}

output "redis_host" {
  description = "Redis instance host"
  value       = google_redis_instance.leaderboard_redis.host
}

output "redis_port" {
  description = "Redis instance port"
  value       = google_redis_instance.leaderboard_redis.port
}

output "telemetry_api_url" {
  description = "Cloud Run Telemetry API endpoint"
  value       = google_cloud_run_service.telemetry_api.status[0].url
}

output "leaderboard_bucket" {
  description = "Storage bucket for leaderboard static files"
  value       = google_storage_bucket.leaderboard_bucket.name
}
