# S3 buckets for raw data lake and Lambda deployment packages.

resource "aws_s3_bucket" "data" {
  bucket        = "${local.name_prefix}-data-${local.name_suffix}"
  force_destroy = true
  tags          = { Name = "${local.name_prefix}-data-bucket" }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket" "lambda" {
  bucket        = "${local.name_prefix}-lambda-${local.name_suffix}"
  force_destroy = true
  tags          = { Name = "${local.name_prefix}-lambda-bucket" }
}

resource "aws_s3_bucket_public_access_block" "lambda" {
  bucket                  = aws_s3_bucket.lambda.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
