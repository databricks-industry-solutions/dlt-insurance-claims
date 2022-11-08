# -------------------------------------------------------------------------------------------------------------------- #
# SQL Endpoint                                                                                                         #
# -------------------------------------------------------------------------------------------------------------------- #

# Declare a set of local variables
locals {
  databricks_sql_endpoint_http_path        = regex("httpPath\\=(.*)\\;", databricks_sql_endpoint.this.jdbc_url)[0]
  databricks_sql_endpoint_port             = regex("jdbc:spark:\\/\\/.*\\:(\\d+)\\/", databricks_sql_endpoint.this.jdbc_url)[0]
  databricks_sql_endpoint_server_host_name = regex("jdbc:spark:\\/\\/(.*)\\:", databricks_sql_endpoint.this.jdbc_url)[0]
}

# Create a DBSQL endpoint to access Lakehouse resources
resource "databricks_sql_endpoint" "this" {
  name = var.stack_name

  auto_stop_mins   = 60
  cluster_size     = "Medium"
  min_num_clusters = 1
  max_num_clusters = 5

  enable_photon             = true
  enable_serverless_compute = true

  channel {
    name = "CHANNEL_NAME_CURRENT"
  }
}

# -------------------------------------------------------------------------------------------------------------------- #
# Workspace Setup                                                                                                      #
# -------------------------------------------------------------------------------------------------------------------- #

# Declare a set of local variables
locals {
  databricks_repo_path = "/Repos/${var.databricks_username}/${var.stack_name}"
  git_repo_name        = regex("([^\\/]+$)", var.git_repository_url)[0]
  schemas_path         = "${path.module}/../data/schemas"
  target_database      = "insurance_demo_lakehouse"
}

# Register the target Git provider credentials for accessing remote repositories
resource "databricks_git_credential" "this" {
  # Check if the resource should be created
  count = var.create_databricks_git_credential ? 1 : 0

  git_username          = var.git_username
  git_provider          = var.git_provider
  personal_access_token = var.git_personal_access_token
}

# Add the git repository containing the Databricks Notebooks to the target workspace
resource "databricks_repo" "this" {
  url          = var.git_repository_url
  git_provider = var.git_provider
  branch       = var.git_repository_branch
  path         = local.databricks_repo_path
}

# Upload the data model for the MongoDB claims dataset to the DBFS storage layer
resource "databricks_dbfs_file" "model_mongodb_claims" {
  source = "${local.schemas_path}/mongodb/claims.json"
  path   = "/${var.stack_name}/schemas/mongodb/claims.json"
}

# Upload the data model for the SQL policies dataset to the DBFS storage layer
resource "databricks_dbfs_file" "model_sql_policies" {
  source = "${local.schemas_path}/sql/policies.json"
  path   = "/${var.stack_name}/schemas/sql/policies.json"
}

# Upload the data model for the external accidents dataset to the DBFS storage layer
resource "databricks_dbfs_file" "model_s3_accidents" {
  source = "${local.schemas_path}/s3/accidents.json"
  path   = "/${var.stack_name}/schemas/s3/accidents.json"
}

# -------------------------------------------------------------------------------------------------------------------- #
# Pipeline Setup                                                                                                       #
# -------------------------------------------------------------------------------------------------------------------- #

# Construct a Delta Live Tables pipeline to process the sample data
resource "databricks_pipeline" "this" {
  depends_on = [
    databricks_repo.this
  ]

  name = var.stack_name

  # Uncomment the block below for interval triggering on a continuous pipeline setup
  # configuration = {
  #   "pipelines.trigger.interval" = "5 minutes"
  # }

  cluster {
    label = "default"

    autoscale {
      min_workers = 1
      max_workers = 5
    }
  }

  library {
    notebook {
      path = "${local.databricks_repo_path}/notebooks/dlt/01-Staging"
    }
  }

  library {
    notebook {
      path = "${local.databricks_repo_path}/notebooks/dlt/02-Curation"
    }
  }

  library {
    notebook {
      path = "${local.databricks_repo_path}/notebooks/dlt/03-Aggregation"
    }
  }

  filters {
    include = ["com.databricks.include"]
    exclude = ["com.databricks.exclude"]
  }

  target     = local.target_database
  continuous = false
}

# Create a scheduled job to trigger the Delta Live Tables pipeline
resource "databricks_job" "this" {
  name = var.stack_name

  pipeline_task {
    pipeline_id = databricks_pipeline.this.id
  }

  schedule {
    pause_status           = "PAUSED"
    quartz_cron_expression = "0 0/5 * * * ?"
    timezone_id            = "UTC"
  }

  email_notifications {
    on_failure                = [var.databricks_username]
    no_alert_for_skipped_runs = false
  }
}

# -------------------------------------------------------------------------------------------------------------------- #
# SQL Queries and Visualisations                                                                                       #
# -------------------------------------------------------------------------------------------------------------------- #

# Construct a SQL query for a view of the issued policies
resource "databricks_sql_query" "issued_policies" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-issued-policies"
  query          = <<EOF
SELECT
  curr.policies_issued AS policies_issued,
  prev.policies_issued AS previous_issued
FROM
  (
    SELECT
      count(DISTINCT policy_number) AS policies_issued
    FROM
      ${local.target_database}.curated_policies
    WHERE
      issue_date BETWEEN '{{ date.start}}' AND '{{ date.end }}'
  ) curr
  JOIN
  (
    SELECT
      count(DISTINCT policy_number) AS policies_issued
    FROM
      ${local.target_database}.curated_policies
    WHERE
      issue_date BETWEEN date_sub('{{ date.start }}', datediff('{{ date.end }}', '{{ date.start }}'))
        AND date_sub('{{ date.end }}', datediff('{{ date.end }}', '{{ date.start }}'))
  ) prev;
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a counter visualisation of the issued-policies query results
resource "databricks_sql_visualization" "counter_issued_policies" {
  query_id    = databricks_sql_query.issued_policies.id
  type        = "counter"
  name        = "Issued Policies"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "counterLabel": "Issued Policies",
    "counterColName": "policies_issued",
    "targetColName": "previous_issued",
    "formatTargetValue": true
  })
}

# Construct a SQL query for a view of the expired policies
resource "databricks_sql_query" "expired_policies" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-expired-policies"
  query          = <<EOF
SELECT
  curr.policies_expired AS policies_expired,
  prev.policies_expired AS previous_expired
FROM
  (
    SELECT
      count(DISTINCT policy_number) AS policies_expired
    FROM
      ${local.target_database}.curated_policies
    WHERE
      expiry_date BETWEEN '{{ date.start}}' AND '{{ date.end }}'
  ) curr
  JOIN
  (
    SELECT
      count(DISTINCT policy_number) AS policies_expired
    FROM
      ${local.target_database}.curated_policies
    WHERE
      expiry_date BETWEEN date_sub('{{ date.start }}', datediff('{{ date.end }}', '{{ date.start }}'))
        AND date_sub('{{ date.end }}', datediff('{{ date.end }}', '{{ date.start }}'))
  ) prev;
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a counter visualisation of the expired-policies query results
resource "databricks_sql_visualization" "counter_expired_policies" {
  query_id    = databricks_sql_query.expired_policies.id
  type        = "counter"
  name        = "Expired Policies"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "counterLabel": "Expired Policies",
    "counterColName": "policies_expired",
    "targetColName": "previous_expired",
    "formatTargetValue": true
  })
}

# Construct a SQL query for a view of the total exposure
resource "databricks_sql_query" "exposure" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-exposure"
  query          = <<EOF
SELECT
  round(curr.total_exposure, 0) AS total_exposure,
  round(prev.total_exposure, 0) AS previous_exposure
FROM
  (
    SELECT
      sum(sum_insured) AS total_exposure
    FROM
      ${local.target_database}.curated_policies
    WHERE
      expiry_date > '{{ date.end }}'
      AND (effective_date <= '{{ date.start }}'
        OR (effective_date BETWEEN '{{ date.start }}' AND '{{ date.end }}'))
  ) curr
  JOIN
  (
    SELECT
      sum(sum_insured) AS total_exposure
    FROM
      ${local.target_database}.curated_policies
    WHERE
      expiry_date > date_sub('{{ date.end }}', datediff('{{ date.end }}', '{{ date.start }}'))
      AND (effective_date <= date_sub('{{ date.start }}', datediff('{{ date.end }}', '{{ date.start }}'))
        OR (effective_date BETWEEN date_sub('{{ date.start }}', datediff('{{ date.end }}', '{{ date.start }}'))
          AND date_sub('{{ date.end }}', datediff('{{ date.end }}', '{{ date.start }}'))))
  ) prev;
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a counter visualisation of the exposure query results
resource "databricks_sql_visualization" "counter_exposure" {
  query_id    = databricks_sql_query.exposure.id
  type        = "counter"
  name        = "Exposure"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "counterLabel": "Exposure",
    "counterColName": "total_exposure",
    "stringPrefix": "$",
    "targetColName": "previous_exposure",
    "formatTargetValue": true
  })
}

# Construct a SQL query for a view of the total exposure
resource "databricks_sql_query" "number_of_claims" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-number-of-claims"
  query          = <<EOF
SELECT
  curr.total_claims AS total_claims,
  prev.total_claims AS previous_claims
FROM
  (
    SELECT
      count(*) AS total_claims
    FROM
      ${local.target_database}.curated_claims
    WHERE
      claim_date BETWEEN '{{ date.start }}' AND '{{ date.end }}'
  ) curr
  JOIN
  (
    SELECT
      count(*) AS total_claims
    FROM
      ${local.target_database}.curated_claims
    WHERE
      claim_date BETWEEN date_sub('{{ date.start }}', datediff('{{ date.end }}', '{{ date.start }}'))
        AND date_sub('{{ date.end }}', datediff('{{ date.end }}', '{{ date.start }}'))
  ) prev;
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a counter visualisation of the number-of-claims query results
resource "databricks_sql_visualization" "counter_number_of_claims" {
  query_id    = databricks_sql_query.number_of_claims.id
  type        = "counter"
  name        = "Number of Claims"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "counterLabel": "Number of Claims",
    "counterColName": "total_claims",
    "targetColName": "previous_claims",
    "formatTargetValue": true
  })
}

# Construct a SQL query for a view of the total claims amount
resource "databricks_sql_query" "claims_amount" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-claims-amount"
  query          = <<EOF
SELECT
  round(curr.total_claims, 0) AS total_claims,
  round(prev.total_claims, 0) AS previous_claims
FROM
  (
    SELECT
      sum(total_claim_amount) AS total_claims
    FROM
      ${local.target_database}.curated_claims
    WHERE
      claim_date BETWEEN '{{ date.start }}' AND '{{ date.end }}'
  ) curr
  JOIN
  (
    SELECT
      sum(total_claim_amount) AS total_claims
    FROM
      ${local.target_database}.curated_claims
    WHERE
      claim_date BETWEEN date_sub('{{ date.start }}', datediff('{{ date.end }}', '{{ date.start }}'))
        AND date_sub('{{ date.end }}', datediff('{{ date.end }}', '{{ date.start }}'))
  ) prev;
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a counter visualisation of the total-claims-amount query results
resource "databricks_sql_visualization" "counter_claims_amount" {
  query_id    = databricks_sql_query.claims_amount.id
  type        = "counter"
  name        = "Total Claims"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "counterLabel": "Total Claims",
    "counterColName": "total_claims",
    "stringPrefix": "$",
    "targetColName": "previous_claims",
    "formatTargetValue": true
  })
}

# Construct a SQL query for a view of the loss ratio
resource "databricks_sql_query" "loss_ratio" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-loss-ratio"
  query          = <<EOF
SELECT
  round(c.total_claims / p.total_premium, 2) AS loss_ratio,
  round(c.previous_claims / p.previous_premium, 2) AS previous_ratio
FROM
  (
    SELECT
      round(curr.total_premium, 0) as total_premium,
      round(prev.total_premium, 0) as previous_premium
    FROM
      (
        SELECT
          sum(premium * months_active) AS total_premium
        FROM
          (
            SELECT
              effective_date,
              expiry_date,
              premium,
              round(months_between(expiry_date, effective_date), 0) AS months_active
            FROM
              ${local.target_database}.curated_policies
            WHERE
              issue_date BETWEEN '{{ date.start }}' AND '{{ date.end }}'
          )
        WHERE
          months_active > 0
      ) curr
      JOIN
      (
        SELECT
          sum(premium * months_active) AS total_premium
        FROM
          (
            SELECT
              effective_date,
              expiry_date,
              premium,
              round(months_between(expiry_date, effective_date), 0) AS months_active
            FROM
              ${local.target_database}.curated_policies
            WHERE
              issue_date BETWEEN date_sub('{{ date.start }}', datediff('{{ date.end }}', '{{ date.start }}'))
                AND date_sub('{{ date.end }}', datediff('{{ date.end }}', '{{ date.start }}'))
          )
        WHERE
          months_active > 0
      ) prev
  ) p
  JOIN
  (
    SELECT
      round(curr.total_claims, 0) AS total_claims,
      round(prev.total_claims, 0) AS previous_claims
    FROM
      (
        SELECT
          sum(total_claim_amount) AS total_claims
        FROM
          ${local.target_database}.curated_claims
        WHERE
          claim_date BETWEEN '{{ date.start }}' AND '{{ date.end }}'
      ) curr
      JOIN
      (
        SELECT
          sum(total_claim_amount) AS total_claims
        FROM
          ${local.target_database}.curated_claims
        WHERE
          claim_date BETWEEN date_sub('{{ date.start }}', datediff('{{ date.end }}', '{{ date.start }}'))
            AND date_sub('{{ date.end }}', datediff('{{ date.end }}', '{{ date.start }}'))
      ) prev
  ) c;
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a counter visualisation of the loss-ratio query results
resource "databricks_sql_visualization" "counter_loss_ratio" {
  query_id    = databricks_sql_query.loss_ratio.id
  type        = "counter"
  name        = "Loss Ratio"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "counterLabel": "Loss Ratio",
    "counterColName": "loss_ratio",
    "targetColName": "previous_ratio",
    "stringDecimal": 2,
    "formatTargetValue": true
  })
}

# Construct a SQL query for a view of accidents vs claimed-for incidents
resource "databricks_sql_query" "accidents_vs_incidents" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-accidents-vs-incidents"
  query          = <<EOF
SELECT
  accident_year_month,
  number_of_accidents,
  number_of_incidents
FROM
  ${local.target_database}.aggregated_accidents_monthly
LEFT JOIN
  (
    SELECT
      date_format(incident_date, 'y-MM') AS incident_year_month,
      count(*) AS number_of_incidents
    FROM
      ${local.target_database}.curated_incidents
    WHERE
      incident_date BETWEEN '{{ date.start }}' AND '{{ date.end }}'
    GROUP BY
      incident_year_month
  ) incidents ON accident_year_month = incident_year_month
WHERE
  accident_year_month BETWEEN cast(date_format('{{ date.start }}', 'y-MM') AS STRING) AND cast(date_format('{{ date.end }}', 'y-MM') AS STRING);
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a combo-chart visualisation showing the number of accidents vs claimed-for incidents
resource "databricks_sql_visualization" "combo_accidents_vs_incidents" {
  query_id    = databricks_sql_query.accidents_vs_incidents.id
  type        = "chart"
  name        = "Accidents vs Incidents"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "version": 2,
    "globalSeriesType": "combo",
    "useAggregationsUi": true,
    "alignYAxesAtZero": true,
    "columnConfigurationMap": {
      "x": {
        "id": "accident_year_month",
        "column": "accident_year_month"
      },
      "y": [
        {
          "id": "number_of_accidents",
          "column": "number_of_accidents",
          "transform": "SUM"
        },
        {
          "id": "number_of_incidents",
          "column": "number_of_incidents",
          "transform": "SUM"
        }
      ]
    },
    "xAxis": {
      "title": {
        "text": "Year / Month"
      }
    },
    "yAxis": [
      {
        "type": "linear",
        "title": {
          "text": "Number of Accidents"
        }
      },
      {
        "type": "linear",
        "title": {
          "text": "Number of Incidents (Claims)"
        },
        "opposite": true
      }
    ],
    "seriesOptions": {
      "number_of_accidents": {
        "type": "column",
        "name": "Number of Accidents"
      },
      "number_of_incidents": {
        "yAxis": 1,
        "type": "line",
        "name": "Number of Incidents (Claims)"
      }
    },
    "legend": {
      "enabled": true,
      "placement": "below"
    }
  })
}

# Construct a SQL query for a view of the aggregated daily claims
resource "databricks_sql_query" "aggregated_claims_daily" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-aggregated-claims-daily"
  query          = <<EOF
SELECT
  claim_date AS `Claim Date`,
  cast(average_incident_hour AS INTEGER) AS `Average Incident Hour`,
  number_of_claims AS `Number of Claims`,
  total_claim_amount AS `Total Claims Amount`,
  injury_claim_amount AS `Injury Claims Amount`,
  property_claim_amount AS `Property Claims Amount`,
  vehicle_claim_amount AS `Vehicle Claims Amount`
FROM
  ${local.target_database}.aggregated_claims_daily
WHERE
  claim_date BETWEEN '{{ date.start }}' AND '{{ date.end }}'
ORDER BY
  claim_date;
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a table visualisation of the aggregated daily claims query results
resource "databricks_sql_visualization" "table_aggregated_claims_daily" {
  query_id    = databricks_sql_query.aggregated_claims_daily.id
  type        = "table"
  name        = "Aggregated Claims - Daily"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "itemsPerPage": 25
  })
}

# Construct a SQL query for a breakdown of incident severity
resource "databricks_sql_query" "breakdown_incident_severity" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-incident-severity"
  query          = <<EOF
SELECT
  incident_severity,
  count(incident_severity) AS number_of_incidents
FROM
  ${local.target_database}.curated_claims
WHERE
  claim_date BETWEEN '{{ date.start }}' AND '{{ date.end }}'
GROUP BY
  incident_severity;
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a pie-chart visualisation for the breakdown of incident severity
resource "databricks_sql_visualization" "pie_breakdown_incident_severity" {
  query_id    = databricks_sql_query.breakdown_incident_severity.id
  type        = "chart"
  name        = "Breakdown - Incident Severity"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "version": 2,
    "globalSeriesType": "pie",
    "useAggregationsUi": true,
    "columnConfigurationMap": {
      "x": {
        "id": "incident_severity",
        "column": "incident_severity"
      },
      "y": [
        {
          "id": "number_of_incidents",
          "column": "number_of_incidents",
          "transform": "SUM"
        }
      ]
    },
    "seriesOptions": {
      "number_of_incidents": {
        "yAxis": 0,
        "type": "pie",
        "name": "Incident Count / Severity"
      }
    }
  })
}

# Construct a SQL query for a view of suspicious claims
resource "databricks_sql_query" "suspicious_claims" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-suspicious-claims"
  query          = <<EOF
SELECT
  *
FROM
  (
    SELECT
      claim_date,
      count(*) as number_of_claims,
      count_if(suspicious_activity) as suspicious_activity,
      cast(avg(lag(count_if(suspicious_activity), 7) OVER (ORDER BY claim_date)) OVER (ORDER BY claim_date) AS INTEGER) AS 7d_avg_suspicious_activity
    FROM
      ${local.target_database}.curated_claims
    WHERE
      claim_date BETWEEN ('{{ date.start }}' - INTERVAL '30' DAYS) AND '{{ date.end }}'
    GROUP BY
      claim_date
    ORDER BY
      claim_date
  )
WHERE
  claim_date BETWEEN '{{ date.start }}' AND '{{ date.end }}'
ORDER BY
  claim_date;
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a area-chart visualisation for the view of suspicious claims
resource "databricks_sql_visualization" "area_suspicious_claims" {
  query_id    = databricks_sql_query.suspicious_claims.id
  type        = "chart"
  name        = "Suspicious Claims"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "version": 2,
    "globalSeriesType": "area",
    "useAggregationsUi": true,
    "alignYAxesAtZero": true,
    "columnConfigurationMap": {
      "x": {
        "id": "claim_date",
        "column": "claim_date"
      },
      "y": [
        {
          "id": "number_of_claims",
          "column": "number_of_claims",
          "transform": "SUM"
        },
        {
          "id": "suspicious_activity",
          "column": "suspicious_activity",
          "transform": "SUM"
        },
        {
          "id": "7d_avg_suspicious_activity",
          "column": "7d_avg_suspicious_activity",
          "transform": "SUM"
        }
      ]
    },
    "xAxis": {
      "title": {
        "text": "Claim Date"
      }
    },
    "yAxis": [
      {
        "type": "linear",
        "title": {
          "text": "Number of Claims"
        }
      }
    ],
    "seriesOptions": {
      "number_of_claims": {
        "type": "area",
        "name": "Total Number of Claims"
      },
      "suspicious_activity": {
        "type": "area",
        "name": "Number of Suspicious Claims"
      },
      "7d_avg_suspicious_activity": {
        "type": "area",
        "name": "Number of Suspicious Claims (7d Rolling Average)"
      }
    },
    "legend": {
      "enabled": true,
      "placement": "below"
    }
  })
}

# Construct a SQL query for a breakdown of the incident types
resource "databricks_sql_query" "breakdown_incident_types" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-incident-types"
  query          = <<EOF
SELECT
  incident_type,
  count(incident_type) AS number_of_incidents
FROM
  ${local.target_database}.curated_claims
WHERE
  claim_date BETWEEN '{{ date.start }}' AND '{{ date.end }}'
GROUP BY
  incident_type;
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a pie-chart visualisation for the breakdown of incident types
resource "databricks_sql_visualization" "pie_breakdown_incident_types" {
  query_id    = databricks_sql_query.breakdown_incident_types.id
  type        = "chart"
  name        = "Breakdown - Incident Type"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "version": 2,
    "globalSeriesType": "pie",
    "useAggregationsUi": true,
    "columnConfigurationMap": {
      "x": {
        "id": "incident_type",
        "column": "incident_type"
      },
      "y": [
        {
          "id": "number_of_incidents",
          "column": "number_of_incidents",
          "transform": "SUM"
        }
      ]
    },
    "seriesOptions": {
      "number_of_incidents": {
        "yAxis": 0,
        "type": "pie",
        "name": "Incident Count / Type"
      }
    }
  })
}

# Construct a SQL query for the average customer tenure at claim
resource "databricks_sql_query" "tenure_at_claim" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-tenure-at-claim"
  query          = <<EOF
SELECT
  curr.avg_tenure_at_claim AS average_tenure_at_claim,
  prev.avg_tenure_at_claim AS previous_tenure_at_claim
FROM
  (
    SELECT
      round(avg(months_as_customer), 0) AS avg_tenure_at_claim
    FROM
      insurance_demo_lakehouse.curated_claims
    WHERE
      claim_date BETWEEN '{{ date.start }}' AND '{{ date.end }}'
  ) curr
  JOIN
  (
    SELECT
      round(avg(months_as_customer), 0) AS avg_tenure_at_claim
    FROM
      insurance_demo_lakehouse.curated_claims
    WHERE
      claim_date BETWEEN date_sub('{{ date.start }}', datediff('{{ date.end }}', '{{ date.start }}'))
        AND date_sub('{{ date.end }}', datediff('{{ date.end }}', '{{ date.start }}'))
  ) prev;
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a counter visualisation of the tenure-at-claim query results
resource "databricks_sql_visualization" "counter_tenure_at_claim" {
  query_id    = databricks_sql_query.tenure_at_claim.id
  type        = "counter"
  name        = "Average Tenure at Claim"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "counterLabel": "Months",
    "counterColName": "average_tenure_at_claim",
    "targetColName": "previous_tenure_at_claim",
    "formatTargetValue": true
  })
}

# Construct a SQL query for the average claim frequency
resource "databricks_sql_query" "claim_frequency" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-claim-frequency"
  query          = <<EOF
SELECT
  round(curr.claims_per_day, 2) AS claims_per_day,
  round(prev.claims_per_day, 2) AS previous_claims_per_day
FROM
  (
    SELECT
      count(DISTINCT claim_number) / datediff('{{ date.end}}', '{{ date.start }}') AS claims_per_day
    FROM
      ${local.target_database}.curated_claims
    WHERE
      claim_date BETWEEN '{{ date.start }}' AND '{{ date.end }}'
  ) curr
  JOIN
  (
    SELECT
      count(DISTINCT claim_number) / datediff('{{ date.end}}', '{{ date.start }}') AS claims_per_day
    FROM
      ${local.target_database}.curated_claims
    WHERE
      claim_date BETWEEN date_sub('{{ date.start }}', datediff('{{ date.end }}', '{{ date.start }}'))
        AND date_sub('{{ date.end }}', datediff('{{ date.end }}', '{{ date.start }}'))
  ) prev;
  EOF

  parameter {
    name  = "date"
    title = "Date"
    date_range {
      value = ""
    }
  }
}

# Create a counter visualisation of the average claim frequency query results
resource "databricks_sql_visualization" "counter_claim_frequency" {
  query_id    = databricks_sql_query.claim_frequency.id
  type        = "counter"
  name        = "Average Claim Frequency"
  description = ""

  # Update the configuration of the visualisation
  options = jsonencode({
    "counterLabel": "Claims per Day",
    "counterColName": "claims_per_day",
    "stringDecimal": 2,
    "targetColName": "previous_claims_per_day",
    "formatTargetValue": true
  })
}

# -------------------------------------------------------------------------------------------------------------------- #
# SQL Dashboards                                                                                                       #
# -------------------------------------------------------------------------------------------------------------------- #

# Create a SQL Dashboard to present the collection of query results
resource "databricks_sql_dashboard" "this" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  name = "Acme Insurance Group - Claims Overview"

  tags = [
    var.stack_name
  ]
}

# Create a widget for the issued-policies counter visualisation
resource "databricks_sql_widget" "counter_issued_policies" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.counter_issued_policies.id

  title = "Issued Policies"

  position {
    pos_x  = 0
    pos_y  = 0
    size_x = 1
    size_y = 5
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# Create a widget for the expired-policies counter visualisation
resource "databricks_sql_widget" "counter_expired_policies" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.counter_expired_policies.id

  title = "Expired Policies"

  position {
    pos_x  = 1
    pos_y  = 0
    size_x = 1
    size_y = 5
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# Create a widget for the total exposure counter visualisation
resource "databricks_sql_widget" "counter_exposure" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.counter_exposure.id

  title = "Exposure"

  position {
    pos_x  = 2
    pos_y  = 0
    size_x = 1
    size_y = 5
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# Create a widget for the total number of claims counter visualisation
resource "databricks_sql_widget" "counter_number_of_claims" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.counter_number_of_claims.id

  title = "Number of Claims"

  position {
    pos_x  = 3
    pos_y  = 0
    size_x = 1
    size_y = 5
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# Create a widget for the total claims amount counter visualisation
resource "databricks_sql_widget" "counter_claims_amount" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.counter_claims_amount.id

  title = "Total Claims"

  position {
    pos_x  = 4
    pos_y  = 0
    size_x = 1
    size_y = 5
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# Create a widget for the loss ratio counter visualisation
resource "databricks_sql_widget" "counter_loss_ratio" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.counter_loss_ratio.id

  title = "Loss Ratio"

  position {
    pos_x  = 5
    pos_y  = 0
    size_x = 1
    size_y = 5
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# Create a widget for the accident vs incidents visualisation
resource "databricks_sql_widget" "combo_accidents_vs_incidents" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.combo_accidents_vs_incidents.id

  title = "Accidents vs Incidents (Claims)"

  position {
    pos_x  = 0
    pos_y  = 2
    size_x = 3
    size_y = 10
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# Create a widget for the aggregated daily-claims visualisation
resource "databricks_sql_widget" "table_aggregated_claims_daily" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.table_aggregated_claims_daily.id

  title = "Aggregated Claims - Daily"

  position {
    pos_x  = 4
    pos_y  = 2
    size_x = 3
    size_y = 10
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# Create a widget for the incident-severity breakdown visualisation
resource "databricks_sql_widget" "pie_breakdown_incident_severity" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.pie_breakdown_incident_severity.id

  title = "Breakdown - Incident Severity"

  position {
    pos_x  = 3
    pos_y  = 12
    size_x = 1
    size_y = 10
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# Create a widget for the suspicious claims visualisation
resource "databricks_sql_widget" "area_suspicious_claims" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.area_suspicious_claims.id

  title = "Suspicious Claims"

  position {
    pos_x  = 0
    pos_y  = 12
    size_x = 3
    size_y = 10
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# Create a widget for the breakdown of incident types visualisation
resource "databricks_sql_widget" "pie_breakdown_incident_types" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.pie_breakdown_incident_types.id

  title = "Breakdown - Incident Type"

  position {
    pos_x  = 4
    pos_y  = 12
    size_x = 1
    size_y = 10
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# Create a widget for the tenure-at-claim counter visualisation
resource "databricks_sql_widget" "counter_tenure_at_claim" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.counter_tenure_at_claim.id

  title = "Average Tenure at Claim"

  position {
    pos_x  = 5
    pos_y  = 12
    size_x = 1
    size_y = 5
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# Create a widget for the average claim-frequency counter visualisation
resource "databricks_sql_widget" "counter_claim_frequency" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  dashboard_id     = databricks_sql_dashboard.this.id
  visualization_id = databricks_sql_visualization.counter_claim_frequency.id

  title = "Average Claim Frequency"

  position {
    pos_x  = 5
    pos_y  = 17
    size_x = 1
    size_y = 5
  }

  parameter {
    type   = "dashboard-level"
    name   = "date"
    map_to = "date"
  }
}

# -------------------------------------------------------------------------------------------------------------------- #
# SQL Alerts                                                                                                           #
# -------------------------------------------------------------------------------------------------------------------- #

# Construct a SQL query for the number of suspicious claims in the past 7 days
resource "databricks_sql_query" "past_week_suspicious_claims" {
  depends_on = [
    databricks_sql_endpoint.this
  ]

  data_source_id = databricks_sql_endpoint.this.data_source_id
  name           = "${var.stack_name}-7d-suspicious-claims"
  query          = <<EOF
SELECT
  count_if(suspicious_activity) as number_of_suspicious_claims
FROM
  ${local.target_database}.curated_claims
WHERE
  claim_date >= current_date() - INTERVAL '7' DAYS;
  EOF
}