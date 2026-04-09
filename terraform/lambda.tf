# Lambda functions for the ETL pipeline.
# Deployment packages are built locally by scripts/package_lambdas.sh.
# Terraform uploads them directly — no S3 pre-upload needed.

locals {
  lambda_zip_dir = "${path.module}/../.build/lambda/zips"
  lambda_runtime = "python3.12"

  # Common VPC config for all Lambdas
  lambda_subnet_ids         = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  lambda_security_group_ids = [aws_security_group.lambda.id]

  # Parsed endpoints (Terraform outputs host:port, Lambdas need them separate)
  rds_host      = split(":", aws_db_instance.postgres.endpoint)[0]
  rds_port      = tostring(aws_db_instance.postgres.port)
  redshift_host = split(":", aws_redshift_cluster.main.endpoint)[0]
  redshift_port = tostring(aws_redshift_cluster.main.port)

  # Common env vars
  rds_env = {
    RDS_HOST     = local.rds_host
    RDS_PORT     = local.rds_port
    RDS_DATABASE = aws_db_instance.postgres.db_name
    RDS_USER     = var.db_username
    RDS_PASSWORD = var.db_password
  }
  redshift_env = {
    REDSHIFT_HOST     = local.redshift_host
    REDSHIFT_PORT     = local.redshift_port
    REDSHIFT_DATABASE = aws_redshift_cluster.main.database_name
    REDSHIFT_USER     = var.db_username
    REDSHIFT_PASSWORD = var.db_password
  }
}

# ── psycopg2 Lambda Layer ───────────────────────────────

resource "aws_lambda_layer_version" "psycopg2" {
  layer_name          = "${local.name_prefix}-psycopg2"
  filename            = "${local.lambda_zip_dir}/psycopg2-layer.zip"
  source_code_hash    = filebase64sha256("${local.lambda_zip_dir}/psycopg2-layer.zip")
  compatible_runtimes = [local.lambda_runtime]
  description         = "psycopg2-binary for PostgreSQL/Redshift access"
}


# ── Schema Init Lambda ──────────────────────────────────

resource "aws_lambda_function" "schema_init" {
  function_name    = "${local.name_prefix}-schema-init"
  role             = aws_iam_role.lambda_execution.arn
  handler          = "schema_init.lambda_handler"
  runtime          = local.lambda_runtime
  timeout          = 300
  memory_size      = 256
  filename         = "${local.lambda_zip_dir}/schema_init.zip"
  source_code_hash = filebase64sha256("${local.lambda_zip_dir}/schema_init.zip")

  layers = [aws_lambda_layer_version.psycopg2.arn]

  vpc_config {
    subnet_ids         = local.lambda_subnet_ids
    security_group_ids = local.lambda_security_group_ids
  }

  environment {
    variables = merge(local.rds_env, local.redshift_env)
  }

  tags = { Name = "${local.name_prefix}-schema-init" }
}

# ── ETL: S3 → RDS ──────────────────────────────────────

resource "aws_lambda_function" "etl_s3_to_rds" {
  function_name    = "${local.name_prefix}-etl-s3-to-rds"
  role             = aws_iam_role.lambda_execution.arn
  handler          = "etl_s3_to_rds_handler.lambda_handler"
  runtime          = local.lambda_runtime
  timeout          = 900
  memory_size      = 512
  filename         = "${local.lambda_zip_dir}/etl_s3_to_rds.zip"
  source_code_hash = filebase64sha256("${local.lambda_zip_dir}/etl_s3_to_rds.zip")

  layers = [aws_lambda_layer_version.psycopg2.arn]

  vpc_config {
    subnet_ids         = local.lambda_subnet_ids
    security_group_ids = local.lambda_security_group_ids
  }

  environment {
    variables = merge(local.rds_env, {
      S3_BUCKET   = aws_s3_bucket.data.id
      S3_PREFIX   = "omop/"
      CUTOFF_DATE = var.cutoff_date
    })
  }

  tags = { Name = "${local.name_prefix}-etl-s3-to-rds" }
}

# ── ETL: RDS → Redshift ────────────────────────────────

resource "aws_lambda_function" "etl_rds_to_redshift" {
  function_name    = "${local.name_prefix}-etl-rds-to-redshift"
  role             = aws_iam_role.lambda_execution.arn
  handler          = "etl_rds_to_redshift_handler.lambda_handler"
  runtime          = local.lambda_runtime
  timeout          = 900
  memory_size      = 512
  filename         = "${local.lambda_zip_dir}/etl_rds_to_redshift.zip"
  source_code_hash = filebase64sha256("${local.lambda_zip_dir}/etl_rds_to_redshift.zip")

  layers = [aws_lambda_layer_version.psycopg2.arn]

  vpc_config {
    subnet_ids         = local.lambda_subnet_ids
    security_group_ids = local.lambda_security_group_ids
  }

  environment {
    variables = merge(local.rds_env, local.redshift_env, {
      ML_REDSHIFT_FUNCTION    = "${local.name_prefix}-ml-redshift"
      ML_COMORBIDITY_FUNCTION = "${local.name_prefix}-ml-comorbidity"
    })
  }

  tags = { Name = "${local.name_prefix}-etl-rds-to-redshift" }
}

# ── Stream Consumer ─────────────────────────────────────

resource "aws_lambda_function" "stream_consumer" {
  function_name    = "${local.name_prefix}-stream-consumer"
  role             = aws_iam_role.lambda_execution.arn
  handler          = "stream_consumer.lambda_handler"
  runtime          = local.lambda_runtime
  timeout          = 60
  memory_size      = 256
  filename         = "${local.lambda_zip_dir}/stream_consumer.zip"
  source_code_hash = filebase64sha256("${local.lambda_zip_dir}/stream_consumer.zip")

  layers = [aws_lambda_layer_version.psycopg2.arn]

  vpc_config {
    subnet_ids         = local.lambda_subnet_ids
    security_group_ids = local.lambda_security_group_ids
  }

  environment {
    variables = local.rds_env
  }

  tags = { Name = "${local.name_prefix}-stream-consumer" }
}

# ── ML: Redshift ML (clustering + risk scoring) ────────
# Uses Redshift's native CREATE MODEL (backed by SageMaker).
# No sklearn or external ML libraries needed.

resource "aws_lambda_function" "ml_redshift" {
  function_name    = "${local.name_prefix}-ml-redshift"
  role             = aws_iam_role.lambda_execution.arn
  handler          = "ml_handler.lambda_handler"
  runtime          = local.lambda_runtime
  timeout          = 900
  memory_size      = 256
  filename         = "${local.lambda_zip_dir}/ml_redshift.zip"
  source_code_hash = filebase64sha256("${local.lambda_zip_dir}/ml_redshift.zip")

  layers = [aws_lambda_layer_version.psycopg2.arn]

  vpc_config {
    subnet_ids         = local.lambda_subnet_ids
    security_group_ids = local.lambda_security_group_ids
  }

  environment {
    variables = merge(local.redshift_env, {
      ML_MODULE        = "ml_redshift"
      S3_BUCKET        = aws_s3_bucket.data.id
      REDSHIFT_IAM_ROLE = aws_iam_role.redshift.arn
    })
  }

  tags = { Name = "${local.name_prefix}-ml-redshift" }
}

# ── ML: Comorbidity ─────────────────────────────────────

resource "aws_lambda_function" "ml_comorbidity" {
  function_name    = "${local.name_prefix}-ml-comorbidity"
  role             = aws_iam_role.lambda_execution.arn
  handler          = "ml_handler.lambda_handler"
  runtime          = local.lambda_runtime
  timeout          = 900
  memory_size      = 512
  filename         = "${local.lambda_zip_dir}/ml_comorbidity.zip"
  source_code_hash = filebase64sha256("${local.lambda_zip_dir}/ml_comorbidity.zip")

  layers = [aws_lambda_layer_version.psycopg2.arn]

  vpc_config {
    subnet_ids         = local.lambda_subnet_ids
    security_group_ids = local.lambda_security_group_ids
  }

  environment {
    variables = merge(local.redshift_env, {
      ML_MODULE = "ml_comorbidity"
    })
  }

  tags = { Name = "${local.name_prefix}-ml-comorbidity" }
}

# ── CloudWatch Log Groups ──────────────────────────────

resource "aws_cloudwatch_log_group" "lambda_logs" {
  for_each = toset([
    "schema-init", "etl-s3-to-rds", "etl-rds-to-redshift",
    "stream-consumer", "ml-redshift", "ml-comorbidity",
  ])

  name              = "/aws/lambda/${local.name_prefix}-${each.key}"
  retention_in_days = 14

  tags = { Name = "${local.name_prefix}-${each.key}-logs" }
}
