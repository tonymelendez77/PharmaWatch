variable "aws_region" {
  description = "AWS region for S3 and IAM resources"
  type        = string
}

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
}

variable "gcp_project_id" {
  description = "Google Cloud project ID for BigQuery"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_username" {
  description = "Snowflake login username"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake login password"
  type        = string
  sensitive   = true
}
