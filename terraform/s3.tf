# --------------------------------------------------------------------------- #
# Iceberg data lake bucket
# --------------------------------------------------------------------------- #

resource "aws_s3_bucket" "iceberg_lake" {
  bucket = var.iceberg_bucket_name

  lifecycle {
    prevent_destroy = true   # protect production audit trail from accidental deletion
  }
}

resource "aws_s3_bucket_versioning" "iceberg_lake" {
  bucket = aws_s3_bucket.iceberg_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "iceberg_lake" {
  bucket = aws_s3_bucket.iceberg_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "iceberg_lake" {
  bucket                  = aws_s3_bucket.iceberg_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "iceberg_lake" {
  bucket = aws_s3_bucket.iceberg_lake.id

  rule {
    id     = "iceberg-tiering"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# --------------------------------------------------------------------------- #
# Terraform remote state bucket (pre-existing; imported, not created here)
# --------------------------------------------------------------------------- #

data "aws_s3_bucket" "terraform_state" {
  bucket = var.terraform_state_bucket
}
