# Lambda triggers — S3 event notifications, Kinesis event source mapping,
# and schema init invocation.

# ── S3 Event Notifications ──────────────────────────────
# All notification rules for a bucket must be in a single resource.

resource "aws_s3_bucket_notification" "data_bucket" {
  bucket = aws_s3_bucket.data.id

  # Trigger ETL when _manifest.json is uploaded after all OMOP CSVs
  lambda_function {
    lambda_function_arn = aws_lambda_function.etl_s3_to_rds.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "omop/"
    filter_suffix       = "_manifest.json"
  }

  # Trigger RDS→Redshift when S3→RDS completes
  lambda_function {
    lambda_function_arn = aws_lambda_function.etl_rds_to_redshift.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "pipeline/"
    filter_suffix       = "s3_to_rds_complete.json"
  }

  depends_on = [
    aws_lambda_permission.s3_invoke_etl_s3_to_rds,
    aws_lambda_permission.s3_invoke_etl_rds_to_redshift,
  ]
}

# ── S3 → Lambda Permissions ─────────────────────────────

resource "aws_lambda_permission" "s3_invoke_etl_s3_to_rds" {
  statement_id  = "AllowS3InvokeETLS3ToRDS"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.etl_s3_to_rds.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data.arn
}

resource "aws_lambda_permission" "s3_invoke_etl_rds_to_redshift" {
  statement_id  = "AllowS3InvokeETLRDSToRedshift"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.etl_rds_to_redshift.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data.arn
}

# ── Kinesis → Stream Consumer ───────────────────────────

resource "aws_lambda_event_source_mapping" "kinesis_to_consumer" {
  event_source_arn  = aws_kinesis_stream.clinical_events.arn
  function_name     = aws_lambda_function.stream_consumer.arn
  starting_position = "LATEST"
  batch_size        = 100

  # Retry configuration
  maximum_retry_attempts             = 3
  bisect_batch_on_function_error     = true
  maximum_record_age_in_seconds      = 3600
  parallelization_factor             = 1
}

# ── Schema Init Invocation ──────────────────────────────
# Runs once after databases and Lambda are created.
# Uses null_resource so it only re-runs if the SQL files or
# database endpoints change.

resource "null_resource" "schema_init" {
  triggers = {
    rds_endpoint         = aws_db_instance.postgres.endpoint
    redshift_endpoint    = aws_redshift_cluster.main.endpoint
    rds_schema_hash      = filemd5("${path.module}/../sql/rds_schema.sql")
    redshift_schema_hash = filemd5("${path.module}/../sql/redshift_schema.sql")
  }

  provisioner "local-exec" {
    command = <<-EOT
      aws lambda invoke \
        --function-name ${aws_lambda_function.schema_init.function_name} \
        --payload '{"action":"init"}' \
        --region ${var.aws_region} \
        /tmp/schema_init_response.json && \
      cat /tmp/schema_init_response.json
    EOT
  }

  depends_on = [
    aws_lambda_function.schema_init,
    aws_db_instance.postgres,
    aws_redshift_cluster.main,
  ]
}

# ── S3 VPC Endpoint ─────────────────────────────────────
# Free gateway endpoint — keeps S3 traffic off the NAT gateway.

resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_vpc.main.id
  service_name = "com.amazonaws.${var.aws_region}.s3"

  route_table_ids = [aws_route_table.private.id]

  tags = { Name = "${local.name_prefix}-s3-endpoint" }
}
