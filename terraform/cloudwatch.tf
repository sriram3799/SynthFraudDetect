resource "aws_cloudwatch_log_group" "audit" {
  name              = "/sentinelstream/audit"
  retention_in_days = 90

  tags = {
    Name = "${var.project_name}-audit-logs-${var.environment}"
  }
}

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/sentinelstream/flink"
  retention_in_days = 30

  tags = {
    Name = "${var.project_name}-flink-logs-${var.environment}"
  }
}

resource "aws_cloudwatch_log_group" "kafka" {
  name              = "/sentinelstream/kafka"
  retention_in_days = 14

  tags = {
    Name = "${var.project_name}-kafka-logs-${var.environment}"
  }
}
