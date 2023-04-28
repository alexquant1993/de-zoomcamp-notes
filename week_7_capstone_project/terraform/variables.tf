variable "credentials" {
  description = "The GCP credentials JSON file path."
  type        = string
}

variable "project_id" {
  description = "The GCP project ID."
  type        = string
  default     = "idealista-scraper-384619"
}

variable "region" {
  description = "The GCP region."
  type        = string
  default     = "europe-southwest1"
}

variable "zone" {
  description = "The zone to host the resources."
  type        = string
  default     = "europe-southwest1-b"
}

variable "machine_type" {
  description = "The machine type for the Prefect VM."
  type        = string
  default     = "e2-medium"
}

variable "vm_image" {
  description = "The image for the Prefect VM."
  type        = string
  default     = "ubuntu-os-cloud/ubuntu-2004-lts"
}

variable "boot_disk_size" {
  description = "The size of the boot disk for the Prefect VM in GB."
  type        = number
  default     = 20
}

variable "vm_ssh_user" {
  description = "The SSH username for the Idealista VM."
  type        = string
}

variable "vm_ssh_pub_key" {
  description = "The path to the SSH public key for the Idealista VM."
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "data_lake_bucket" {
  description = "Name of the data lake - GCS, where scraped data will be placed to"
  type        = string
  default     = "idealista_data_lake"
}

variable "bq_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "idealista_listings"
}