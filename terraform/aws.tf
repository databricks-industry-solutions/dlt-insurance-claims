# -------------------------------------------------------------------------------------------------------------------- #
# Blob Storage                                                                                                         #
# -------------------------------------------------------------------------------------------------------------------- #

# Declare a set of local variables
locals {
  s3_object_key_accidents = "accidents/sample.csv.gz"
  s3_object_key_claims    = "claims/sample.json"
}

# Define a set of S3 bucket resources to provide the necessary layers for storage
module "s3_bucket" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "3.2.1"

  # Iterate over the set of storage layer identifiers to create separate resources
  for_each = toset(["external", "tmp"])

  bucket = "${var.stack_name}-${each.key}-${var.aws_region}"

  # Set the bucket public access block configuration
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm = "AES256"
      }
    }
  }

  force_destroy = true
}

# Upload the claims dataset to the temporary blob storage layer
resource "aws_s3_object" "claims" {
  depends_on = [
    module.s3_bucket
  ]

  bucket = module.s3_bucket["tmp"].s3_bucket_id
  key    = local.s3_object_key_claims
  source = "../data/samples/s3/tmp/claims.json"
}

# Upload the vehicle accidents dataset to the external blob storage layer
resource "aws_s3_object" "accidents" {
  depends_on = [
    module.s3_bucket
  ]

  bucket = module.s3_bucket["external"].s3_bucket_id
  key    = local.s3_object_key_accidents
  source = "../data/samples/s3/external/accidents.csv.gz"
}

# -------------------------------------------------------------------------------------------------------------------- #
# Relational Database                                                                                                  #
# -------------------------------------------------------------------------------------------------------------------- #

# Declare a set of local variables
locals {
  mysql_database_name           = "master"
  mysql_host                    = regex("(.*)\\:", aws_db_instance.mysql.endpoint)[0]
  mysql_port                    = 3306
}

# Create a security group to administer traffic to and from the RDS instance
resource "aws_security_group" "rds" {
  name        = "mysql"
  description = "default MySQL security group"

  vpc_id = data.aws_vpc.default.id

  ingress {
    from_port       = local.mysql_port
    to_port         = local.mysql_port
    protocol        = "tcp"
    security_groups = [data.aws_security_group.default.id]
  }

  ingress {
    from_port       = local.mysql_port
    to_port         = local.mysql_port
    protocol        = "tcp"
    cidr_blocks       = ["0.0.0.0/0"]
    ipv6_cidr_blocks  = ["::/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = var.stack_name
  }
}

# Generate a random password for the database administrator user
resource "random_password" "mysql_admin" {
  length  = 32
  special = false
}

# Create an RDS instance to serve as the enterprise data warehouse resource
resource "aws_db_instance" "mysql" {
  depends_on = [
    aws_security_group.rds
  ]

  identifier = var.stack_name

  engine         = "mysql"
  engine_version = "8.0.25"
  instance_class = "db.t3.micro"

  db_name  = local.mysql_database_name
  username = "admin"
  password = random_password.mysql_admin.result
  port     = 3306

  allocated_storage     = 16
  max_allocated_storage = 32

  multi_az               = false
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = true

  backup_retention_period = 0
  skip_final_snapshot     = true
  deletion_protection     = false
}

# Create a local provisioner to configure the database and upload the policies dataset
resource "null_resource" "prepare_mysql" {
  depends_on = [
    aws_db_instance.mysql
  ]

  provisioner "local-exec" {
    command = <<EOT
      mysql --host=${local.mysql_host} \
        --user=admin \
        --password=${random_password.mysql_admin.result} \
        --execute="CREATE USER fivetran@'%' IDENTIFIED WITH mysql_native_password BY '${random_password.mysql_admin.result}';"
      mysql --host=${local.mysql_host} \
        --user=admin \
        --password=${random_password.mysql_admin.result} \
        master < ../setup/sql/mysql/config.sql
    EOT
  }
}

# -------------------------------------------------------------------------------------------------------------------- #
# Access Credentials                                                                                                   #
# -------------------------------------------------------------------------------------------------------------------- #

# Declare a set of local variables
locals {
  fivetran_credentials_secret_name = "${lower(var.stack_name)}/credentials/fivetran"
  mongodb_credentials_secret_name  = "${lower(var.stack_name)}/credentials/mongodb"
  mysql_credentials_secret_name    = "${lower(var.stack_name)}/credentials/mysql"
}

# Create a secret for storing the access credentials for the EDW instance
resource "aws_secretsmanager_secret" "mysql_credentials" {
  name        = local.mysql_credentials_secret_name
  description = "Credentials used to authenticate and authorise access to EDW database resources"

  recovery_window_in_days = 0
}

# Update the details of the RDS credentials secret
resource "aws_secretsmanager_secret_version" "mysql_credentials" {
  depends_on = [
    aws_db_instance.mysql
  ]

  secret_id     = aws_secretsmanager_secret.mysql_credentials.id
  secret_string = jsonencode({
    "HOST": local.mysql_host,
    "PORT": local.mysql_port,
    "DATABASE": local.mysql_database_name,
    "USERNAME": "admin",
    "PASSWORD": random_password.mysql_admin.result
  })
}

# Create a Secret resource for storing the access credentials for the MongoDB instance
resource "aws_secretsmanager_secret" "mongodb_credentials" {
  name        = local.mongodb_credentials_secret_name
  description = "Credentials used to authenticate and authorise access to MongoDB database resources"

  recovery_window_in_days = 0
}

# Update the details of the Secret resource
resource "aws_secretsmanager_secret_version" "mongodb_credentials" {
  secret_id     = aws_secretsmanager_secret.mongodb_credentials.id
  secret_string = jsonencode({
    "HOST": local.create_mongodbatlas_resources ? local.mongodbatlas_host : var.mongodb_host,
    "USERNAME": local.create_mongodbatlas_resources ? "m_admin" : var.mongodb_username_readwrite,
    "PASSWORD": local.create_mongodbatlas_resources ? random_password.mongodbatlas_admin[0].result : var.mongodb_password_readwrite
  })
}

# Create a secret for storing the access credentials for the Fivetran API service
resource "aws_secretsmanager_secret" "fivetran_credentials" {
  name        = local.fivetran_credentials_secret_name
  description = "Credentials used to authenticate and authorise access to Fivetran resources"

  recovery_window_in_days = 0
}

# Update the details of the Fivetran credentials secret
resource "aws_secretsmanager_secret_version" "fivetran_credentials" {
  secret_id     = aws_secretsmanager_secret.fivetran_credentials.id
  secret_string = jsonencode({
    "API_KEY": var.fivetran_api_key,
    "API_SECRET": var.fivetran_api_secret
  })
}

# -------------------------------------------------------------------------------------------------------------------- #
# Claim Events                                                                                                         #
# -------------------------------------------------------------------------------------------------------------------- #

# Declare a set of local variables
locals {
  aws_account_id                 = data.aws_caller_identity.this.account_id
  claim_event_rule_name          = "${var.stack_name}-claims"
  claim_submit_function_repository_name = "${var.stack_name}/lambda/function/claim/submit"
  claim_submit_function_source_path     = "../src/lambda/functions/claim/submit"
}

# Create an Event Bridge rule to trigger uploads of new claim records
resource "aws_cloudwatch_event_rule" "claim_submit" {
  name        = local.claim_event_rule_name
  description = "Scheduled event used to trigger submission of new claim records"

  schedule_expression = "rate(1 minute)"
  is_enabled          = false
}

# Crete a resource for storing the Lambda function image
resource "aws_ecr_repository" "claim_submit_function" {
  name = local.claim_submit_function_repository_name
}

# Create a lifecycle policy to expire ECR images beyond a certain count
resource "aws_ecr_lifecycle_policy" "claim_submit_function" {
  repository = aws_ecr_repository.claim_submit_function.name

  policy = <<EOF
    {
      "rules": [
        {
          "rulePriority": 1,
          "description": "Keep only the last untagged images",
          "selection": {
            "tagStatus": "untagged",
            "countType": "imageCountMoreThan",
            "countNumber": 1
          },
          "action": {
            "type": "expire"
          }
        }
      ]
    }
  EOF
}

# Create a local provisioner to build the Lambda function image
resource "null_resource" "claim_submit_function" {
  triggers = {
    src_hash = data.archive_file.claim_submit_function.output_sha
  }

  provisioner "local-exec" {
    command = <<EOF
      aws ecr get-login-password --region ${var.aws_region} | \
        docker login --username AWS --password-stdin ${local.aws_account_id}.dkr.ecr.${var.aws_region}.amazonaws.com
      cd ${local.claim_submit_function_source_path}
      docker build --platform linux/amd64 -t ${aws_ecr_repository.claim_submit_function.repository_url}:latest .
      docker push ${aws_ecr_repository.claim_submit_function.repository_url}:latest
    EOF
  }
}

# Create an IAM Role resource granting permissions to the claims function
resource "aws_iam_role" "claim_submit_function" {
  name = "${var.stack_name}-claim-submit-function"

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

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/SecretsManagerReadWrite",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  ]
}

# Define a resource block for the Lambda Function
resource "aws_lambda_function" "claim_submit" {
  depends_on = [
    data.aws_ecr_image.claim_submit_function
  ]

  function_name = "${var.stack_name}-claim-submit"
  description   = "Function invoked to trigger submission of new claims records"

  role         = aws_iam_role.claim_submit_function.arn
  image_uri    = "${aws_ecr_repository.claim_submit_function.repository_url}@${data.aws_ecr_image.claim_submit_function.id}"
  package_type = "Image"
  timeout      = 60

  environment {
    variables = {
      BUCKET_NAME     = module.s3_bucket["tmp"].s3_bucket_id
      COLLECTION_NAME = local.mongodb_collection_name
      DATABASE_NAME   = local.mongodb_database_name
      KEY             = local.s3_object_key_claims
      RULE_NAME       = local.claim_event_rule_name
      SECRET_NAME     = local.mongodb_credentials_secret_name
    }
  }
}

# Define an Event target to trigger the claims function using the schedule event rule
resource "aws_cloudwatch_event_target" "claim_submit_function" {
  rule      = aws_cloudwatch_event_rule.claim_submit.name
  target_id = "claim"
  arn       = aws_lambda_function.claim_submit.arn
}

# Update the claims function permissions to allow invocation from EventBridge rules
resource "aws_lambda_permission" "claim_submit_function" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.claim_submit.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.claim_submit.arn
}

# -------------------------------------------------------------------------------------------------------------------- #
# Fivetran Connector Setup                                                                                             #
# -------------------------------------------------------------------------------------------------------------------- #

# Create an IAM Role granting permission to Fivetran for accessing S3 resources
resource "aws_iam_role" "s3_fivetran" {
  name = "${var.stack_name}-s3-fivetran"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::834469178297:root"
        }
        Condition = {
          "StringEquals" = {
            "sts:ExternalId" = fivetran_group.databricks.id
          }
        }
      }
    ]
  })

  inline_policy {
    name   = "AmazonS3PermissionsForFivetran"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Action   = [
            "s3:Get*",
            "s3:List*"
          ]
          Effect   = "Allow"
          Resource = [
            module.s3_bucket["external"].s3_bucket_arn,
            "${module.s3_bucket["external"].s3_bucket_arn}/*"
          ]
        },
      ]
    })
  }
}