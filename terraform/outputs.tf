# -------------------------------------------------------------------------------------------------------------------- #
# Databricks                                                                                                           #
# -------------------------------------------------------------------------------------------------------------------- #

output "databricks_sql_endpoint_data_source_id" {
  description = "Unique identifier of the SQL Endpoint data source."
  value       = databricks_sql_endpoint.this.data_source_id
}

output "databricks_sql_dashboard_id" {
  description = "Unique identifier of the SQL Dashboard."
  value       = databricks_sql_dashboard.this.id
}

# -------------------------------------------------------------------------------------------------------------------- #
# Fivetran                                                                                                             #
# -------------------------------------------------------------------------------------------------------------------- #

output "fivetran_group_databricks_id" {
  description = "Unique identifier for the Fivetran group."
  value       = fivetran_group.databricks.id
}