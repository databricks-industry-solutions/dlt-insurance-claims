terraform {
  required_version = ">= 0.14.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 4.6.0"
    }

    databricks = {
      source  = "databrickslabs/databricks"
      version = ">= 0.5.5"
    }

    fivetran = {
      source  = "fivetran/fivetran"
      version = ">= 0.4.3"
    }

    mongodbatlas = {
      source  = "mongodb/mongodbatlas"
      version = ">= 1.3.1"
    }
  }

  # Uncomment line below for deployment using GitLab CI/CD pipeline
  # backend "http" { }
}

provider "aws" {
  region  = var.aws_region
}

provider "databricks" {
  host  = var.databricks_workspace_url
  token = var.databricks_personal_access_token
}

provider "fivetran" {
  api_key    = var.fivetran_api_key
  api_secret = var.fivetran_api_secret
}

provider "mongodbatlas" { }