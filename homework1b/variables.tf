locals {
  data_lake_bucket = "dtc-data-lake"
  folder_id="b1g8ubj63leku74cq5jd"
}


variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "ru-central1-a"
  type = string
}
