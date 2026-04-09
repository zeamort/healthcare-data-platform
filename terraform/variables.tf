# Input variables for the Healthcare Data Platform infrastructure.
# Sensitive values (db_password) should be provided via terraform.tfvars or TF_VAR_ env vars.

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "db_username" {
  description = "Master username for RDS and Redshift"
  type        = string
  default     = "postgres"
}

variable "db_password" {
  description = "Master password for RDS and Redshift. Provide via TF_VAR_db_password or terraform.tfvars"
  type        = string
  sensitive   = true
}

variable "rds_instance_class" {
  description = "RDS instance type"
  type        = string
  default     = "db.t3.micro"
}

variable "redshift_node_type" {
  description = "Redshift node type"
  type        = string
  default     = "dc2.large"
}

variable "redshift_number_of_nodes" {
  description = "Number of Redshift nodes"
  type        = number
  default     = 1
}

variable "cutoff_date" {
  description = "Batch/streaming split date (YYYY-MM-DD). Records before this date load in batch; after go to streaming."
  type        = string
  default     = "2020-01-01"
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access databases (set to your IP for security, e.g. [\"203.0.113.5/32\"])"
  type        = list(string)
}
