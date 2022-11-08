# Create a group for associating Fivetran resources
resource "fivetran_group" "databricks" {
  name = replace(var.stack_name, "-", "_")
}

# Create a destination to deliver data to Delta tables hosted through Databricks
resource "fivetran_destination" "databricks" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  group_id           = fivetran_group.databricks.id
  service            = "databricks_aws"
  time_zone_offset   = "0"
  region             = "AWS_${replace(upper(var.aws_region), "-", "_")}"
  trust_certificates = "true"
  trust_fingerprints = "true"
  run_setup_tests    = "true"

  config {
    server_host_name       = local.databricks_sql_endpoint_server_host_name
    port                   = local.databricks_sql_endpoint_port
    http_path              = local.databricks_sql_endpoint_http_path
    personal_access_token  = var.databricks_personal_access_token
    create_external_tables = false
  }
}

# -------------------------------------------------------------------------------------------------------------------- #
# Blob Storage                                                                                                         #
# -------------------------------------------------------------------------------------------------------------------- #

# Create a connector to ingest data from the external-data S3 storage layer
resource "fivetran_connector" "s3_external_accidents" {
  depends_on = [
    aws_s3_object.accidents,
    fivetran_destination.databricks,
    module.s3_bucket
  ]

  group_id           = fivetran_group.databricks.id
  service            = "s3"
  sync_frequency     = 60
  paused             = false
  pause_after_trial  = true
  run_setup_tests    = true
  trust_certificates = true

  destination_schema {
    name   = "insurance_demo_s3"
    table  = "accidents"
  }

  config {
    connection_type = "Directly"
    external_id     = fivetran_group.databricks.id
    bucket          = module.s3_bucket["external"].s3_bucket_id
    prefix          = try(trimsuffix(regex("^(.*[\\/])", local.s3_object_key_accidents)[0], "/"), "")
    role_arn        = aws_iam_role.s3_fivetran.arn
    file_type       = "csv"
    compression     = "gzip"
  }
}

# -------------------------------------------------------------------------------------------------------------------- #
# Relational Database                                                                                                  #
# -------------------------------------------------------------------------------------------------------------------- #

# Create a connector to ingest data from the enterprise data warehouse
resource "fivetran_connector" "mysql" {
  depends_on = [
    aws_db_instance.mysql,
    aws_security_group.rds,
    fivetran_destination.databricks,
    null_resource.prepare_mysql
  ]

  group_id           = fivetran_group.databricks.id
  service            = "mysql_rds"
  sync_frequency     = 720
  paused             = false
  pause_after_trial  = true
  run_setup_tests    = true
  trust_certificates = true

  destination_schema {
    name   = "insurance_demo_mysql"
    prefix = "insurance_demo_mysql"
  }

  config {
    connection_type = "Directly"
    host            = local.mysql_host
    port            = local.mysql_port
    user            = "admin"
    password        = random_password.mysql_admin.result
    database        = local.mysql_database_name
    update_method   = "TELEPORT"
  }
}

# -------------------------------------------------------------------------------------------------------------------- #
# MongoDB Atlas                                                                                                        #
# -------------------------------------------------------------------------------------------------------------------- #

# Create a connector to ingest data from the MongoDB claims database
resource "fivetran_connector" "mongodbatlas" {
  depends_on = [
    fivetran_destination.databricks,
    mongodbatlas_cluster.default,
    null_resource.prepare_mongodb
  ]

  group_id           = fivetran_group.databricks.id
  service            = "mongo"
  sync_frequency     = 5
  paused             = false
  pause_after_trial  = true
  run_setup_tests    = true
  trust_certificates = true

  destination_schema {
    name   = "insurance_demo_mongodb"
    prefix = "insurance_demo_mongodb"
  }

  config {
    connection_type = "Directly"
    hosts           = ["mongodb+srv://${local.create_mongodbatlas_resources ? local.mongodbatlas_host : var.mongodb_host}/"]
    user            = local.create_mongodbatlas_resources ? "fivetran" : var.mongodb_username_read
    password        = local.create_mongodbatlas_resources ? random_password.mongodbatlas_fivetran[0].result : var.mongodb_password_read
  }
}