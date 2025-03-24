provider "aws" {
  region = "eu-north-1"  # Replace with your preferred region
}

# S3 Bucket for BLS Dataset and Data USA API JSON
resource "aws_s3_bucket" "data_bucket" {
  bucket = "data-pipeline-bucket"  # Replace with your bucket name
}

# Set ACL separately (Fix for deprecated argument)
resource "aws_s3_bucket_acl" "data_bucket_acl" {
  bucket = aws_s3_bucket.data_bucket.id
  acl    = "public-read"
}

# IAM Role for Lambda Functions
resource "aws_iam_role" "lambda_role" {
  name = "lambda_execution_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy for Lambda Functions
resource "aws_iam_role_policy" "lambda_policy" {
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.data_bucket.arn,
          "${aws_s3_bucket.data_bucket.arn}/*"
        ]
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action = [
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage"
        ]
        Effect   = "Allow"
        Resource = aws_sqs_queue.data_queue.arn
      }
    ]
  })
}

# Lambda Function for Part 1 and Part 2
resource "aws_lambda_function" "data_pipeline_lambda" {
  function_name = "data_pipeline_lambda"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.8"

  filename         = "lambda_function.zip"
  source_code_hash = filebase64sha256("lambda_function.zip")

  environment {
    variables = {
      S3_BUCKET = aws_s3_bucket.data_bucket.bucket
    }
  }
}

# CloudWatch Event Rule to Trigger Lambda Daily
resource "aws_cloudwatch_event_rule" "daily_trigger" {
  name                = "daily_trigger"
  schedule_expression = "cron(0 2 * * ? *)"  # Runs daily at 2 AM UTC
}

# CloudWatch Event Target to Invoke Lambda
resource "aws_cloudwatch_event_target" "invoke_lambda" {
  rule      = aws_cloudwatch_event_rule.daily_trigger.name
  target_id = "lambda_target"
  arn       = aws_lambda_function.data_pipeline_lambda.arn
}

# SQS Queue for S3 Notifications
resource "aws_sqs_queue" "data_queue" {
  name = "data-pipeline-queue"
}

# S3 Event Notification to Populate SQS
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.data_bucket.id

  queue {
    queue_arn = aws_sqs_queue.data_queue.arn
    events    = ["s3:ObjectCreated:*"]
    filter_suffix = ".json"
  }
}

# Ensure sqs_processor.zip exists before Terraform execution
resource "null_resource" "ensure_lambda_package" {
  provisioner "local-exec" {
    command = <<EOT
      if [ ! -f "sqs_processor.zip" ]; then
        echo "Error: sqs_processor.zip not found!" >&2
        exit 1
      fi
    EOT
  }
}

# Lambda Function to Process SQS Messages
resource "aws_lambda_function" "sqs_processor_lambda" {
  function_name = "sqs_processor_lambda"
  role          = aws_iam_role.lambda_role.arn
  handler       = "sqs_processor.lambda_handler"
  runtime       = "python3.8"

  filename         = "sqs_processor.zip"
  source_code_hash = filebase64sha256("sqs_processor.zip")

  depends_on = [null_resource.ensure_lambda_package]  # Ensures the file exists
}

# SQS Trigger for Lambda
resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = aws_sqs_queue.data_queue.arn
  function_name    = aws_lambda_function.sqs_processor_lambda.arn
}
