output "s3_bucket_name" {
  description = "Name of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.bucket
}

output "iam_role_arn" {
  description = "ARN of the Spark IAM role"
  value       = aws_iam_role.spark_role.arn
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.pharmawatch.dataset_id
}

output "snowflake_warehouse_name" {
  description = "Snowflake warehouse name"
  value       = snowflake_warehouse.pharmawatch.name
}
