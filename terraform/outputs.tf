# Outputs for use by ETL scripts, API, and documentation.
# Sensitive values are marked — use `terraform output -raw <name>` to retrieve them.

output "s3_data_bucket" {
  description = "S3 data bucket name"
  value       = aws_s3_bucket.data.id
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint (host:port)"
  value       = aws_db_instance.postgres.endpoint
}

output "rds_database" {
  description = "RDS database name"
  value       = aws_db_instance.postgres.db_name
}

output "redshift_endpoint" {
  description = "Redshift cluster endpoint (host:port)"
  value       = aws_redshift_cluster.main.endpoint
}

output "redshift_database" {
  description = "Redshift database name"
  value       = aws_redshift_cluster.main.database_name
}

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "lambda_role_arn" {
  description = "Lambda execution role ARN"
  value       = aws_iam_role.lambda_execution.arn
}

output "lambda_security_group_id" {
  description = "Lambda security group ID"
  value       = aws_security_group.lambda.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs (for Lambda VPC config)"
  value       = [aws_subnet.private_1.id, aws_subnet.private_2.id]
}
