# Declare a set of local variables
locals {
  create_mongodbatlas_resources  = !(length(var.mongodb_host) > 0 && length(var.mongodb_username_readwrite) > 0 && length(var.mongodb_password_readwrite) > 0)
  mongodbatlas_cluster_name      = "default"
  mongodbatlas_host              = try(regex("mongodb\\+srv\\:\\/\\/(.*)", mongodbatlas_cluster.default[0].connection_strings.0.standard_srv)[0], "")
  mongodb_collection_name        = "claims"
  mongodb_database_name          = "master"
}

# Define a Project resource to house the collection of MongoDB Atlas resources
resource "mongodbatlas_project" "this" {
  # Check if the resource should be created
  count = local.create_mongodbatlas_resources ? 1 : 0

  name   = var.stack_name
  org_id = var.mongodbatlas_org_id

  api_keys {
    api_key_id = var.mongodbatlas_api_key_id
    role_names = [
      "GROUP_CLUSTER_MANAGER",
      "GROUP_DATA_ACCESS_ADMIN",
      "GROUP_DATA_ACCESS_READ_WRITE",
      "GROUP_OWNER"
    ]
  }
}

# Allow public access to the MongoDB Atlas project
resource "mongodbatlas_project_ip_access_list" "public" {
  # Check if the resource should be created
  count = local.create_mongodbatlas_resources ? 1 : 0

  project_id = mongodbatlas_project.this[0].id
  cidr_block = "0.0.0.0/0"
  comment    = "Allow access from anywhere"
}

# Grant explicit access to Fivetran services
resource "mongodbatlas_project_ip_access_list" "fivetran" {
  # Iterate over the set of provided IP addresses
  for_each = local.create_mongodbatlas_resources ? toset(var.fivetran_ip_addresses) : []

  project_id = mongodbatlas_project.this[0].id
  cidr_block = each.value
  comment    = "Allow access to Fivetran"
}

resource "mongodbatlas_cluster" "default" {
  # Check if the resource should be created
  count = local.create_mongodbatlas_resources ? 1 : 0

  project_id   = mongodbatlas_project.this[0].id
  name         = local.mongodbatlas_cluster_name
  cluster_type = "REPLICASET"

  replication_specs {
    num_shards = 1
    regions_config {
      region_name     = replace(upper(var.aws_region), "-", "_")
      electable_nodes = 3
      priority        = 7
      read_only_nodes = 0
    }
  }

  cloud_backup                 = true
  auto_scaling_disk_gb_enabled = true
  mongo_db_major_version       = "5.0"
  disk_size_gb                 = 20
  provider_name                = "AWS"
  provider_instance_size_name  = "M10"

  auto_scaling_compute_enabled                    = true
  auto_scaling_compute_scale_down_enabled         = true
  provider_auto_scaling_compute_min_instance_size = "M10"
  provider_auto_scaling_compute_max_instance_size = "M20"

  advanced_configuration {
    minimum_enabled_tls_protocol = "TLS1_2"
    oplog_size_mb                = 10240
  }
}

# Generate a random password for the database administrator user
resource "random_password" "mongodbatlas_admin" {
  # Check if the resource should be created
  count = local.create_mongodbatlas_resources ? 1 : 0

  length  = 32
  special = false
}

# Create a MondoDB Atlas database user to provide administrator access
resource "mongodbatlas_database_user" "admin" {
  # Check if the resource should be created
  count = local.create_mongodbatlas_resources ? 1 : 0

  auth_database_name = "admin"
  project_id         = mongodbatlas_project.this[0].id
  username           = "m_admin"
  password           = random_password.mongodbatlas_admin[0].result

  roles {
    role_name     = "atlasAdmin"
    database_name = "admin"
  }

  roles {
    role_name     = "dbAdminAnyDatabase"
    database_name = "admin"
  }

  roles {
    role_name     = "readWriteAnyDatabase"
    database_name = "admin"
  }

  scopes {
    name = local.mongodbatlas_cluster_name
    type = "CLUSTER"
  }
}

# Generate a random password for the Fivetran user
resource "random_password" "mongodbatlas_fivetran" {
  # Check if the resource should be created
  count = local.create_mongodbatlas_resources ? 1 : 0

  length  = 32
  special = false
}

# Create a MondoDB Atlas database user to provide Fivetran access
resource "mongodbatlas_database_user" "fivetran" {
  # Check if the resource should be created
  count = local.create_mongodbatlas_resources ? 1 : 0

  auth_database_name = "admin"
  project_id         = mongodbatlas_project.this[0].id
  username           = "fivetran"
  password           = random_password.mongodbatlas_fivetran[0].result

  roles {
    role_name     = "readAnyDatabase"
    database_name = "admin"
  }

  roles {
    role_name     = "read"
    database_name = "local"
  }

  scopes {
    name = local.mongodbatlas_cluster_name
    type = "CLUSTER"
  }
}

# Create a local provisioner to configure the database setup and upload the default dataset
resource "null_resource" "prepare_mongodb" {
  # Check if the resource should be created
  count = local.create_mongodbatlas_resources ? 1 : 0

  depends_on = [
    mongodbatlas_cluster.default
  ]

  provisioner "local-exec" {
    command = <<EOF
      cd ../setup/mongodb
      docker build --platform linux/amd64 -t mongodb:latest .
      docker run \
        -v "${abspath(path.root)}/../data/samples/mongodb:/data" \
        -e HOST=${local.mongodbatlas_host} \
        -e USERNAME=m_admin \
        -e PASSWORD=${random_password.mongodbatlas_admin[0].result} \
        -e DATABASE=${local.mongodb_database_name} \
        -e COLLECTION=${local.mongodb_collection_name} \
        --name mongodb \
        mongodb
    EOF
  }
}