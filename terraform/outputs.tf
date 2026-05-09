output "iceberg_bucket_arn" {
  description = "ARN of the Iceberg S3 data lake bucket."
  value       = aws_s3_bucket.iceberg_lake.arn
}

output "iceberg_bucket_name" {
  description = "Name of the Iceberg S3 data lake bucket."
  value       = aws_s3_bucket.iceberg_lake.id
}

output "flink_instance_profile_arn" {
  description = "ARN of the IAM instance profile to attach to Flink EC2 nodes."
  value       = aws_iam_instance_profile.flink.arn
}

output "flink_role_arn" {
  description = "ARN of the IAM role assumed by Flink EC2 nodes."
  value       = aws_iam_role.flink.arn
}

output "vpc_id" {
  description = "ID of the SentinelStream VPC."
  value       = aws_vpc.main.id
}

output "bastion_public_ip" {
  description = "Public IP of the bastion host for SSH tunnelling."
  value       = aws_instance.bastion.public_ip
}

output "kafka_private_ip" {
  description = "Private IP of the Kafka EC2 node (accessible via bastion tunnel)."
  value       = aws_instance.kafka.private_ip
}
