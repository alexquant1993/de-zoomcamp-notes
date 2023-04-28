terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  credentials = file(var.credentials)
}

# Virtual machine
resource "google_compute_instance" "idealista_vm" {
  name         = "idealista-pipeline-vm"
  machine_type = var.machine_type
  zone         = var.zone

  boot_disk {
    initialize_params {
      size = var.boot_disk_size
      image = var.vm_image
    }
  }

  network_interface {
    network = "default"
    subnetwork = "default"
    access_config {
      // Ephemeral IP
    }
  }

  metadata = {
    ssh-keys = "${var.vm_ssh_user}:${file(var.vm_ssh_pub_key)}"
  }

  tags = ["terraform", "ubuntu"]
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-idealista" {
  name          = "${var.data_lake_bucket}_${var.project_id}" 
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }
}

# Data Warehouse
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset
  project    = var.project_id
  location   = var.region
}
