output "gcs_bucket_name" {
  description = "GCS data lake bucket name"
  value       = google_storage_bucket.data_lake.name
}

output "gcs_bucket_url" {
  description = "GCS bucket URL"
  value       = google_storage_bucket.data_lake.url
}

output "bq_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.kbeauty.dataset_id
}


