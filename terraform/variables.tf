variable "aws_region" {
  description = "AWS region for all resources. Choose a region close to your team to minimise latency."
  type        = string
}

variable "project_name" {
  description = "Short project identifier applied to all resource names and tags."
  type        = string
  default     = "sentinelstream"
}

variable "environment" {
  description = "Deployment environment: dev | staging | prod."
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "owner" {
  description = "Team or individual responsible for these resources (used in cost allocation tags)."
  type        = string
}

variable "iceberg_bucket_name" {
  description = "Name of the S3 bucket used as the Iceberg warehouse. Must be globally unique."
  type        = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.iceberg_bucket_name))
    error_message = "iceberg_bucket_name must be a valid S3 bucket name (lowercase, 3-63 chars)."
  }
}

variable "terraform_state_bucket" {
  description = "Name of the pre-existing S3 bucket used to store Terraform remote state."
  type        = string
}

variable "admin_cidr" {
  description = "CIDR block allowed to SSH into the bastion host. Restrict to your office/VPN CIDR."
  type        = string
  validation {
    condition     = can(cidrnetmask(var.admin_cidr))
    error_message = "admin_cidr must be a valid CIDR block (e.g. 203.0.113.0/24)."
  }
}
