variable "credentials" {
  description = "My Credentials"
  default     = "xzde-20240114-d30904c66aaa.json"
}


variable "region" {
  description = "Region"
  default     = "us-west2"
}

variable "project" {
  description = "Project"
  default     = "xzde-20240114"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My Storage Bucket Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "demo-bucket-123456-ximin-zhao"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}