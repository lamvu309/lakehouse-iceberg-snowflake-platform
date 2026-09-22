output "s3_bucket_name" {
  description = "S3 Data Lake bucket name (set as S3_BUCKET_NAME in GitHub Secrets)"
  value       = aws_s3_bucket.data_lake.id
}

output "s3_bucket_arn" {
  description = "S3 Data Lake bucket ARN (used for Snowflake External Volume)"
  value       = aws_s3_bucket.data_lake.arn
}

output "spark_iam_role_arn" {
  description = "IAM Role ARN for PySpark (assign to EC2 instance profile)"
  value       = aws_iam_role.spark_role.arn
}

output "iceberg_warehouse_uri" {
  description = "S3A URI for Iceberg warehouse (set as ICEBERG_WAREHOUSE env var)"
  value       = "s3a://${aws_s3_bucket.data_lake.id}/iceberg"
}

output "ec2_public_ip" {
  description = "EC2 instance public IP (if created)"
  value       = var.create_ec2 ? aws_instance.spark_ec2[0].public_ip : null
}
