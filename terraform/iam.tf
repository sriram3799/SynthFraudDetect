# --------------------------------------------------------------------------- #
# IAM role + instance profile for Flink EC2 nodes
# Permissions are scoped to the Iceberg S3 bucket only — no wildcard access.
# --------------------------------------------------------------------------- #

data "aws_iam_policy_document" "flink_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flink" {
  name               = "${var.project_name}-flink-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.flink_assume_role.json
  description        = "Allows Flink EC2 nodes to read/write the Iceberg S3 lake."
}

data "aws_iam_policy_document" "flink_s3" {
  statement {
    sid    = "IcebergLakeReadWrite"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject",    # needed for Iceberg compaction and snapshot expiry
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.iceberg_lake.arn,
      "${aws_s3_bucket.iceberg_lake.arn}/*",
    ]
  }

  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
    ]
    resources = ["arn:aws:logs:${var.aws_region}:*:log-group:/sentinelstream/*"]
  }
}

resource "aws_iam_role_policy" "flink_s3" {
  name   = "flink-iceberg-s3-access"
  role   = aws_iam_role.flink.id
  policy = data.aws_iam_policy_document.flink_s3.json
}

resource "aws_iam_instance_profile" "flink" {
  name = "${var.project_name}-flink-profile-${var.environment}"
  role = aws_iam_role.flink.name
}
