# Main Terraform configuration for Healthcare Data Platform
# Manages provider setup, random naming suffix, and shared locals.

terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "healthcare-data-platform"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

resource "random_string" "suffix" {
  length  = 8
  special = false
  upper   = false
}

locals {
  name_prefix = "healthcare-${var.environment}"
  name_suffix = random_string.suffix.result
}
