output "data_lake_bucket" {
  description = "Name of the S3 bucket for the raw and processed layers"
  value       = aws_s3_bucket.data_lake.bucket
}