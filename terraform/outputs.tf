output "bucket_arn" {
  description = "ARN of the data pipeline S3 bucket"
  value       = aws_s3_bucket.pipeline.arn
}

output "glue_database_name" {
  description = "Name of the Glue catalog database"
  value       = aws_glue_catalog_database.portfolio.name
}

output "athena_workgroup_name" {
  description = "Name of the Athena workgroup"
  value       = aws_athena_workgroup.portfolio.name
}
