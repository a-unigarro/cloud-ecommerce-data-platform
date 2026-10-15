terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

# No credentials here: the provider reads the AWS_PROFILE environment variable

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      project    = "cloud-ecommerce-data-platform"
      managed_by = "terraform"
    }
  }
}