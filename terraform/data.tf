# Get the unique identifier of the AWS account
data "aws_caller_identity" "this" { }

# Get a list of the available availability zones for the specified AWS region
data "aws_availability_zones" "this" {}

# Get the default VPC resource
data "aws_vpc" "default" {
  default = true
}

# Get the security group associated with the default VPC
data "aws_security_group" "default" {
  vpc_id = data.aws_vpc.default.id

  filter {
    name   = "group-name"
    values = ["default"]
  }
}

# Create a temporary archive of the function resources
data "archive_file" "claim_submit_function" {
  type        = "zip"
  source_dir  = local.claim_submit_function_source_path
  output_path = "${replace(local.claim_submit_function_source_path, "src", "tmp")}.zip"
}

# Create a copy of the built Docker image in the allocated repository
data "aws_ecr_image" "claim_submit_function" {
  depends_on = [
    aws_ecr_repository.claim_submit_function,
    null_resource.claim_submit_function
  ]
  repository_name = local.claim_submit_function_repository_name
  image_tag       = "latest"
}