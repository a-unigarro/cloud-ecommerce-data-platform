# S3 bucket names are global across all AWS accounts
resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-${random_id.suffix.hex}"

  # remove for real production data.
  force_destroy = true
}

# Block every form of public access. Nothing in this project should ever be public.
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}