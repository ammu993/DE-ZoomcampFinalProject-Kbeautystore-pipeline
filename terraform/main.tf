terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

# ── GCS Data Lake Bucket ──────────────────────────────────────
resource "google_storage_bucket" "data_lake" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 90 }   # auto-delete raw files after 90 days
  }

  versioning {
    enabled = false
  }
}

# Folder structure (GCS uses prefixes, not real folders)
resource "google_storage_bucket_object" "raw_orders_prefix" {
  name    = "raw/orders/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "placeholder"
}

resource "google_storage_bucket_object" "raw_products_prefix" {
  name    = "raw/products/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "placeholder"
}

resource "google_storage_bucket_object" "jobs_prefix" {
  name    = "jobs/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "placeholder"
}

resource "google_storage_bucket_object" "temp_prefix" {
  name    = "bq_temp/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "placeholder"
}

# ── BigQuery Dataset ──────────────────────────────────────────
resource "google_bigquery_dataset" "kbeauty" {
  dataset_id                 = var.bq_dataset_id
  friendly_name              = "K-Beauty Pipeline"
  description                = "Staging, dimension, fact and mart tables for the K-Beauty pipeline"
  location                   = var.location
  delete_contents_on_destroy = true
}


# ── IAM roles for the service account ────────────────────────
locals {
  sa_roles = [
    "roles/storage.admin",          # read/write GCS
    "roles/bigquery.admin",         # create/write BQ tables
    "roles/dataproc.admin",         # create/submit Dataproc jobs
    "roles/compute.viewer",         # Dataproc needs this
    "roles/iam.serviceAccountUser", # allows Dataproc to use this SA
  ]
}



