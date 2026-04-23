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

output "kinesis_stream_name" {
  description = "Kinesis stream name for clinical events"
  value       = aws_kinesis_stream.clinical_events.name
}

output "kinesis_stream_arn" {
  description = "Kinesis stream ARN"
  value       = aws_kinesis_stream.clinical_events.arn
}

output "lambda_bucket" {
  description = "S3 bucket for Lambda deployment packages"
  value       = aws_s3_bucket.lambda.id
}

output "schema_init_function" {
  description = "Schema init Lambda function name"
  value       = aws_lambda_function.schema_init.function_name
}

output "etl_s3_to_rds_function" {
  description = "S3→RDS ETL Lambda function name"
  value       = aws_lambda_function.etl_s3_to_rds.function_name
}

output "etl_rds_to_redshift_function" {
  description = "RDS→Redshift ETL Lambda function name"
  value       = aws_lambda_function.etl_rds_to_redshift.function_name
}

output "dashboard_url" {
  description = "Dashboard ALB URL"
  value       = "http://${aws_lb.dashboard.dns_name}"
}

output "ecr_repository_url" {
  description = "ECR repository URL for the dashboard image"
  value       = aws_ecr_repository.dashboard.repository_url
}

output "api_endpoint" {
  description = "REST API base URL (API Gateway v2 HTTP API invoke URL)"
  value       = aws_apigatewayv2_stage.default.invoke_url
}

output "api_function_name" {
  description = "Lambda function name for the API"
  value       = aws_lambda_function.api.function_name
}

