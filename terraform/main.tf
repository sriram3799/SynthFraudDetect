terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state in S3. The bucket must exist before `terraform init`.
  # Bootstrap it once with:
  #   aws s3api create-bucket --bucket <state_bucket> --region <region>
  #   aws s3api put-bucket-versioning --bucket <state_bucket> \
  #     --versioning-configuration Status=Enabled
  backend "s3" {
    bucket = var.terraform_state_bucket   # supplied via terraform.tfvars (gitignored)
    key    = "sentinelstream/terraform.tfstate"
    region = var.aws_region
    encrypt = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}
