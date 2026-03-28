variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west4"
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "EU"
}

variable "credentials_file" {
  description = "Path to GCP service account key JSON"
  type        = string
}

variable "gcs_bucket_name" {
  description = "Name of the GCS data lake bucket"
  type        = string
}

variable "bq_dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "kbeauty_pipeline"
}
