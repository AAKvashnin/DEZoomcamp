terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
    }
  }
}

provider "yandex" {
  folder_id = local.folder_id
  zone = var.region
}

resource "yandex_iam_service_account" "sa" {
 folder_id = local.folder_id
 name = "sa123"
}

resource "yandex_resourcemanager_folder_iam_member" "sa-editor" {
  folder_id = local.folder_id
  role      = "storage.editor"
  member    = "serviceAccount:${yandex_iam_service_account.sa.id}"
}

resource "yandex_iam_service_account_static_access_key" "sa-static-key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "static access key for object storage"
}



# Data Lake Bucket
resource "yandex_storage_bucket" "data-lake-bucket" {
  access_key = yandex_iam_service_account_static_access_key.sa-static-key.access_key
 secret_key = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
  bucket          = "${local.data_lake_bucket}" # Concatenating DL bucket & Project name for unique naming



  force_destroy = true
}

