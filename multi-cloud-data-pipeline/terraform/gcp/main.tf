# GCP Infrastructure for Multi-Cloud Data Pipeline
# This module creates essential GCP resources for data engineering

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Variables
variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "multicloud-pipeline"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "labels" {
  description = "Labels to apply to all resources"
  type        = map(string)
  default = {
    project     = "multi-cloud-pipeline"
    managed-by  = "terraform"
  }
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "dataflow.googleapis.com",
    "pubsub.googleapis.com",
    "sqladmin.googleapis.com",
    "cloudscheduler.googleapis.com",
    "composer.googleapis.com"
  ])
  
  service            = each.value
  disable_on_destroy = false
}

# Cloud Storage Buckets
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-${var.project_name}-data-lake-${var.environment}"
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  
  labels = var.labels
}

resource "google_storage_bucket" "temp" {
  name          = "${var.project_id}-${var.project_name}-temp-${var.environment}"
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  
  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
  
  labels = var.labels
}

resource "google_storage_bucket" "staging" {
  name          = "${var.project_id}-${var.project_name}-staging-${var.environment}"
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  
  labels = var.labels
}

# BigQuery Dataset
resource "google_bigquery_dataset" "analytics" {
  dataset_id  = "analytics_${var.environment}"
  project     = var.project_id
  location    = var.region
  description = "Analytics dataset for multi-cloud pipeline"
  
  default_table_expiration_ms = null
  
  labels = var.labels
}

# BigQuery Tables
resource "google_bigquery_table" "sales" {
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = "sales"
  project    = var.project_id
  
  deletion_protection = false
  
  time_partitioning {
    type  = "DAY"
    field = "transaction_date"
  }
  
  clustering = ["product_id", "region"]
  
  schema = jsonencode([
    {
      name = "transaction_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "transaction_date"
      type = "DATE"
      mode = "REQUIRED"
    },
    {
      name = "product_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "customer_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "amount"
      type = "FLOAT64"
      mode = "REQUIRED"
    },
    {
      name = "quantity"
      type = "INTEGER"
      mode = "REQUIRED"
    },
    {
      name = "region"
      type = "STRING"
      mode = "NULLABLE"
    }
  ])
  
  labels = var.labels
}

resource "google_bigquery_table" "realtime_events" {
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = "realtime_events"
  project    = var.project_id
  
  deletion_protection = false
  
  time_partitioning {
    type  = "HOUR"
    field = "event_timestamp"
  }
  
  schema = jsonencode([
    {
      name = "event_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "event_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "event_type"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "user_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "properties"
      type = "JSON"
      mode = "NULLABLE"
    }
  ])
  
  labels = var.labels
}

# Pub/Sub Topics
resource "google_pubsub_topic" "user_events" {
  name = "user-events-${var.environment}"
  
  labels = var.labels
}

resource "google_pubsub_subscription" "user_events_sub" {
  name  = "user-events-sub-${var.environment}"
  topic = google_pubsub_topic.user_events.name
  
  ack_deadline_seconds = 20
  
  message_retention_duration = "604800s" # 7 days
  
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
  
  labels = var.labels
}

# Cloud SQL Instance (PostgreSQL)
resource "google_sql_database_instance" "main" {
  name             = "${var.project_name}-db-${var.environment}"
  database_version = "POSTGRES_15"
  region           = var.region
  
  deletion_protection = false
  
  settings {
    tier              = "db-f1-micro"
    availability_type = "ZONAL"
    
    backup_configuration {
      enabled            = true
      start_time         = "03:00"
      point_in_time_recovery_enabled = true
    }
    
    ip_configuration {
      ipv4_enabled = true
    }
    
    user_labels = var.labels
  }
}

resource "google_sql_database" "analytics" {
  name     = "analytics"
  instance = google_sql_database_instance.main.name
}

# Service Account for Dataflow
resource "google_service_account" "dataflow" {
  account_id   = "${var.project_name}-dataflow-${var.environment}"
  display_name = "Dataflow Service Account"
  description  = "Service account for Dataflow jobs"
}

# IAM bindings for service account
resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

resource "google_project_iam_member" "bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

resource "google_project_iam_member" "storage_object_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

# Cloud Scheduler Jobs (for scheduled pipelines)
resource "google_cloud_scheduler_job" "daily_pipeline" {
  name             = "daily-pipeline-${var.environment}"
  description      = "Trigger daily data pipeline"
  schedule         = "0 2 * * *" # Run at 2 AM daily
  time_zone        = "America/New_York"
  attempt_deadline = "320s"
  
  http_target {
    http_method = "POST"
    uri         = "https://dataflow.googleapis.com/v1b3/projects/${var.project_id}/locations/${var.region}/templates:launch"
    
    body = base64encode(jsonencode({
      jobName = "daily-pipeline"
    }))
    
    oauth_token {
      service_account_email = google_service_account.dataflow.email
    }
  }
}

# Outputs
output "project_id" {
  description = "GCP Project ID"
  value       = var.project_id
}

output "data_lake_bucket" {
  description = "Data lake bucket name"
  value       = google_storage_bucket.data_lake.name
}

output "temp_bucket" {
  description = "Temporary bucket name"
  value       = google_storage_bucket.temp.name
}

output "bigquery_dataset" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.analytics.dataset_id
}

output "pubsub_topic" {
  description = "Pub/Sub topic name"
  value       = google_pubsub_topic.user_events.name
}

output "pubsub_subscription" {
  description = "Pub/Sub subscription name"
  value       = google_pubsub_subscription.user_events_sub.name
}

output "cloudsql_instance" {
  description = "Cloud SQL instance name"
  value       = google_sql_database_instance.main.name
}

output "dataflow_service_account" {
  description = "Dataflow service account email"
  value       = google_service_account.dataflow.email
}
