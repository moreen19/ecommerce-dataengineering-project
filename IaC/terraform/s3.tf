# S3 Data Lake Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = var.s3_bucket_name
  tags = {
    Name = "Data Lake"
    Project = "Ecommerce DataOps"
  }
}

# S3 Public Access Block (Security Best Practice)
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# AWS Glue Database (used by Athena)
resource "aws_glue_catalog_database" "ecommerce_data_lake" {
  name = var.glue_database_name
}