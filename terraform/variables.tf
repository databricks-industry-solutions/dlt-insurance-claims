# -------------------------------------------------------------------------------------------------------------------- #
# General                                                                                                              #
# -------------------------------------------------------------------------------------------------------------------- #

# Define an input variable for the name of the collection of resources
variable "stack_name" {
  type        = string
  description = "(Optional) Name for the collection of resources."
  default     = "e2-demo-gtm-insurance"
}

# -------------------------------------------------------------------------------------------------------------------- #
# AWS                                                                                                                  #
# -------------------------------------------------------------------------------------------------------------------- #

# Define an input variable for the target AWS region
variable "aws_region" {
  type        = string
  description = "(Optional) Name of the target AWS region."
  default     = "us-east-1"
}

# -------------------------------------------------------------------------------------------------------------------- #
# Databricks                                                                                                           #
# -------------------------------------------------------------------------------------------------------------------- #

# Define an input variable to specify whether the remote Git credentials should be registered with Databricks
variable "create_databricks_git_credential" {
  type        = bool
  description = "(Optional) Specifies whether a new Git credential should be registered with the Databricks workspace. Requires specification of the `git_username` and `git_personal_access_token` variables."
  default     = false
}

# Define an input variable for the URL of the target Databricks workspace
variable "databricks_workspace_url" {
  type        = string
  description = "URL for the target Databricks workspace."
  default     = "https://e2-demo-field-eng.cloud.databricks.com/"
}

# Define an input variable for the personal access token used to access Databricks resources
variable "databricks_personal_access_token" {
  type        = string
  description = "Personal access token used to authenticate Databricks API requests."
}

# Define an input variable for the Databricks account username
variable "databricks_username" {
  type        = string
  description = "Username associated with the target Databricks workspace."
}

# -------------------------------------------------------------------------------------------------------------------- #
# Fivetran                                                                                                             #
# -------------------------------------------------------------------------------------------------------------------- #

# Define an input variable for the API key used to authenticate Fivetran service requests
variable "fivetran_api_key" {
  type        = string
  description = "API key used to authenticate Fivetran service requests."
}

# Define an input variable for the API key secret used to authenticate Fivetran service requests
variable "fivetran_api_secret" {
  type        = string
  description = "Secret associated with the Fivetran API key."
}

# Define an input variable for the list of Fivetran static IP addresses
variable "fivetran_ip_addresses" {
  type        = list(string)
  description = "(Optional) List of static IP addresses used by Fivetran."
  default = [
    "3.239.194.48/29",
    "52.0.2.4/32"
  ]
}

# -------------------------------------------------------------------------------------------------------------------- #
# Git Provider                                                                                                         #
# -------------------------------------------------------------------------------------------------------------------- #

# Define an input variable for the remote Git personal access token
variable "git_personal_access_token" {
  type        = string
  description = "Personal access token for accessing remote Git resources."
  default     = ""
}

# Define an input variable for the name of the Git provider
variable "git_provider" {
  type        = string
  description = "(Optional) Name of the target Git provider."
  default     = "gitLab"

  validation {
    condition = contains([
      "awsCodeCommit",
      "azureDevOpsServices",
      "bitbucketCloud",
      "bitbucketServer",
      "gitHub",
      "gitHubEnterprise",
      "gitLab",
      "gitLabEnterpriseEdition",
    ], var.git_provider)
    error_message = "Invalid value provided for variable 'git_provider'."
  }
}

# Define an input variable for the remote Git account username
variable "git_username" {
  type        = string
  description = "Username for the remote Git account."
  default     = ""
}

# Define an input variable for the name of the source Git branch
variable "git_repository_branch" {
  type        = string
  description = "(Optional) Name of the source Git branch."
  default     = "main"
}

# Define an input variable for the URL of the source code repository
variable "git_repository_url" {
  type = string
  description = "(Optional) URL for the target Git repository."
  default     = "https://gitlab.com/databricks-financial-services/demos/insurance/motor-vehicle-claims"
}

# -------------------------------------------------------------------------------------------------------------------- #
# MongoDB                                                                                                              #
# -------------------------------------------------------------------------------------------------------------------- #

# Define an input variable for the MongoDB Atlas API key
variable "mongodbatlas_api_key_id" {
  type        = string
  description = "(Optional) The unique identifier of the Programmatic API key you want to associate with the Project. The Programmatic API key and Project must share the same parent organization. Note: this is not the `publicKey` of the Programmatic API key but the `id` of the key."
  default     = ""
}

# Define an input variable for the unique identifier of the MongoDB Atlas organization
variable "mongodbatlas_org_id" {
  type        = string
  description = "(Optional) The unique ID of the MongoDB Atlas organization within which to create resources."
  default     = ""
}

# Define an input variable for the host name of an existing MongoDB database instance
variable "mongodb_host" {
  type        = string
  description = "(Optional) Host name for an existing MongoDB instance."
  default     = ""
}

# Define an input variable for the port of an existing MongoDB database instance
variable "mongodb_port" {
  type        = number
  description = "(Optional) Port number for an existing MongoDB instance."
  default     = 27017
}

# Define an input variable for the username of an existing MongoDB database with permissions to read and write records
variable "mongodb_username_readwrite" {
  type        = string
  description = "(Optional) Username for accessing an existing MongoDB database with read/write permission."
  default     = ""
}

# Define an input variable for the password of an existing MongoDB database with permissions to read and write records
variable "mongodb_password_readwrite" {
  type        = string
  description = "(Optional) Password for accessing an existing MongoDB database with read/write permission."
  default     = ""
}

# Define an input variable for the username of an existing MongoDB database with permissions to read records
variable "mongodb_username_read" {
  type        = string
  description = "(Optional) Username for accessing an existing MongoDB database with read-only permission."
  default     = ""
}

# Define an input variable for the password of an existing MongoDB database with permissions to read records
variable "mongodb_password_read" {
  type        = string
  description = "(Optional) Password for accessing an existing MongoDB database with read-only permission."
  default     = ""
}