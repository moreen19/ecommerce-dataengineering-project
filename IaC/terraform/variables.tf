variable "aws_region" {
  description = "AWS region for the deployment"
  type        = string
  default     = "us-east-2"
}

variable "s3_bucket_name" {
  description = "The globally unique S3 bucket name for the data lake"
  type        = string
}

variable "glue_database_name" {
  description = "Name for the AWS Glue Catalog Database"
  type        = string
  default     = "ecommerce_data_lake"
}