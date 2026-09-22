variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "ap-southeast-1"
}

variable "bucket_name" {
  description = "S3 bucket name prefix (account ID appended automatically)"
  type        = string
  default     = "lakehouse-iceberg-data"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod"
  }
}

variable "create_ec2" {
  description = "Whether to create an EC2 instance for PySpark"
  type        = bool
  default     = false
}

variable "ec2_instance_type" {
  description = "EC2 instance type for PySpark workloads"
  type        = string
  default     = "m5.xlarge"
}

variable "ec2_ami_id" {
  description = "AMI ID for EC2 (Amazon Linux 2023 recommended)"
  type        = string
  default     = "ami-0abcdef1234567890"  # Replace with your region's AL2023 AMI
}

variable "ec2_key_name" {
  description = "EC2 key pair name for SSH access"
  type        = string
  default     = ""
}

variable "github_repo" {
  description = "GitHub repository in format 'owner/repo-name' (used for OIDC trust policy)"
  type        = string
  # Example: "lamvu309/lakehouse-iceberg-snowflake-platform"
}
