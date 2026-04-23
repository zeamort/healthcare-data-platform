# REST API — FastAPI on Lambda, fronted by API Gateway v2 (HTTP API).
# The FastAPI app lives in api/; lambda/handlers/api_handler.py wraps it
# with Mangum so it can respond to API Gateway's payload format 2.0.

# ── Lambda function ────────────────────────────────────

resource "aws_lambda_function" "api" {
  function_name    = "${local.name_prefix}-api"
  role             = aws_iam_role.lambda_execution.arn
  handler          = "api_handler.handler"
  runtime          = local.lambda_runtime
  timeout          = 30
  memory_size      = 512
  filename         = "${local.lambda_zip_dir}/api.zip"
  source_code_hash = filebase64sha256("${local.lambda_zip_dir}/api.zip")

  layers = [aws_lambda_layer_version.psycopg2.arn]

  vpc_config {
    subnet_ids         = local.lambda_subnet_ids
    security_group_ids = local.lambda_security_group_ids
  }

  environment {
    variables = merge(local.rds_env, {
      API_KEYS = var.api_keys
    })
  }

  tags = { Name = "${local.name_prefix}-api" }
}

resource "aws_cloudwatch_log_group" "api_lambda" {
  name              = "/aws/lambda/${local.name_prefix}-api"
  retention_in_days = 14
  tags              = { Name = "${local.name_prefix}-api-logs" }
}

# ── HTTP API (API Gateway v2) ──────────────────────────

resource "aws_apigatewayv2_api" "api" {
  name          = "${local.name_prefix}-api"
  protocol_type = "HTTP"
  description   = "Healthcare Data Platform REST API (FastAPI on Lambda)"

  cors_configuration {
    # The Streamlit dashboard doesn't call this API (it queries Redshift
    # directly), so CORS is only relevant for external browser clients.
    # Leave permissive for the capstone; tighten for production.
    allow_origins = ["*"]
    allow_methods = ["GET", "POST", "OPTIONS"]
    allow_headers = ["Content-Type", "X-API-Key"]
    max_age       = 300
  }
}

resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
  timeout_milliseconds   = 30000
}

resource "aws_apigatewayv2_route" "default" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_burst_limit = 100
    throttling_rate_limit  = 50
  }

  tags = { Name = "${local.name_prefix}-api-stage" }
}

resource "aws_lambda_permission" "apigw_invoke_api" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api.execution_arn}/*/*"
}
