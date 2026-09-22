terraform {
  required_version = ">= 1.7.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── Data ─────────────────────────────────────────────────────────────────────

data "aws_caller_identity" "current" {}

locals {
  bucket_name = "${var.bucket_name}-${data.aws_caller_identity.current.account_id}"
  common_tags = {
    Project     = "lakehouse-iceberg"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ── S3 Data Lake ─────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "data_lake" {
  bucket = local.bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle: move raw data to Glacier after 90 days
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "archive-raw-zone"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# ── IAM OIDC Provider for GitHub Actions ─────────────────────────────────────

data "tls_certificate" "github" {
  url = "https://token.actions.githubusercontent.com"
}

resource "aws_iam_openid_connect_provider" "github_actions" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = data.tls_certificate.github.certificates[*].sha1_fingerprint
  tags            = local.common_tags
}

# ── IAM Role for PySpark (EC2 + GitHub Actions OIDC) ─────────────────────────

resource "aws_iam_role" "spark_role" {
  name = "${var.environment}-lakehouse-spark-role"
  tags = local.common_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # EC2 instances (PySpark jobs running on EC2)
        Effect    = "Allow"
        Principal = { Service = "ec2.amazonaws.com" }
        Action    = "sts:AssumeRole"
      },
      {
        # GitHub Actions via OIDC (no long-lived credentials needed)
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.github_actions.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
          StringLike = {
            # Chỉ cho phép repo cụ thể của bạn — thay YOUR_GITHUB_USERNAME
            "token.actions.githubusercontent.com:sub" = "repo:${var.github_repo}:*"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "spark_s3_policy" {
  name = "spark-s3-rw-policy"
  role = aws_iam_role.spark_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "IcebergS3ReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "spark_profile" {
  name = "${var.environment}-spark-instance-profile"
  role = aws_iam_role.spark_role.name
}

# ── EC2 for PySpark (optional for Phase 1 dev) ───────────────────────────────

resource "aws_instance" "spark_ec2" {
  count = var.create_ec2 ? 1 : 0

  ami                  = var.ec2_ami_id
  instance_type        = var.ec2_instance_type
  iam_instance_profile = aws_iam_instance_profile.spark_profile.name
  key_name             = var.ec2_key_name

  user_data = file("${path.module}/../scripts/bootstrap_ec2.sh")

  tags = merge(local.common_tags, {
    Name = "${var.environment}-spark-ingestion"
  })
}
