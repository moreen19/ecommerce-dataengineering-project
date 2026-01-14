# IAM Role for Glue/Athena/Spark Access
resource "aws_iam_role" "data_lake_access" {
  name = "data-lake-access-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com" # Simplification: Assume an EC2 instance or other service will use this
          # For PySpark locally, we rely on the host machine's credentials, 
          # but this IAM role is needed by Glue/Athena for cross-service calls.
        }
      },
    ]
  })
}

# Policy to allow S3, Glue, and Athena access
resource "aws_iam_role_policy" "s3_glue_athena_access" {
  name = "s3-glue-athena-access-policy"
  role = aws_iam_role.data_lake_access.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject"
        ],
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:*"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "athena:*"
        ],
        Resource = "*"
      }
    ]
  })
}