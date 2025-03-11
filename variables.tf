variable "google_credentials" {
    description = "GCP credentials"
    default = "./keys/gcp_cred.json" # path to service account json
}

variable "project" {
  description = "Project"
  default     = "loyal-karma-453205-v6"
}

variable "region" {
  description = "Region"
  default     = "asia-southeast1-a"
}

variable "location" {
  description = "Project Location"
  default     = "asia-southeast1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "de-zoomcamp-453205"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}