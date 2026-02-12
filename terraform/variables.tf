variable "bucket_name" {
  description = "S3 bucket for the data pipeline"
  type        = string
  default     = "weather-pipeline-eric-2026"
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}
