# Databricks notebook source
# MAGIC %md
# MAGIC # Background
# MAGIC 
# MAGIC Fivetran is used to ingest data from internal and external data sources. 
# MAGIC 
# MAGIC Internal data includes policy records housed in the enterprise data warehouse (a MySQL instance hosted on [AWS RDS](https://aws.amazon.com/rds/)), and claims records sourced directly from the production application database (a MongoDB instance hosted on [Atlas](https://www.mongodb.com/atlas/database)). External sources includes traffic accident data for New York City (stored as blob files in [AWS S3](https://aws.amazon.com/s3/)).
# MAGIC 
# MAGIC Policy and accident records are ingested at 12- and 1-hour intervals respectively due to limitations in the refresh rate of source data. Having direct access to the claims application database means new records can be ingested at more regular (5-minute) intervals. Fivetran connectors delivers data directly to Delta Lake tables through a Databricks SQL Endpoint connection.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Setup
# MAGIC 
# MAGIC A Delta Live Tables (DLT) pipeline is configured to read data from the Delta Tables maintained by Fivetran and create a `staging` layer, also known as the `bronze` layer.

# COMMAND ----------

# Load the required libraries
import dlt

# COMMAND ----------

# Declare a global constant for the layer identifier
LAYER = "bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Utility Methods
# MAGIC 
# MAGIC The process of staging data is the same for all internal and external sources. Data is loaded from the source Delta tables and parsed to remove any attributes relating to Fivetran metadata. The `stage_records` method can be reused to create different tables in the DLT `staging` layer.

# COMMAND ----------

# Load the classes required for type hinting
from pyspark.sql import DataFrame

# COMMAND ----------

def stage_records(schema: str, table: str) -> DataFrame:
    """
    Stage records received from source systems for downstream processing.

    Parameters
    ----------
    schema: str
        Name of the source schema.

    table: str
        Name of the source database table.

    Returns
    -------
    sdf: pyspark.sql.DataFrame
        Prepared record set ready for staging.
    """

    # Read the source dataset delivered by Fivetran
    sdf = spark.read.format("delta").load(f"dbfs:/user/hive/warehouse/{schema}.db/{table}")

    # Get a subset of columns to retain in the staged dataset
    cols = [col for col in sdf.columns if "fivetran" not in col and not col.startswith("_")]
    # Select the subset of columns and return the result set
    return sdf.select(*cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Sources

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policies
# MAGIC 
# MAGIC Policy records are read from the `insurance_demo_mysql_master` schema and delivered to the DLT `staging` layer:

# COMMAND ----------

@dlt.table(
    name             = "staged_policies",
    comment          = "Staged policy records",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def stage_policies():
    # Stage the source policy records delivered to the Lakehouse by Fivetran
    return stage_records(schema = "insurance_demo_mysql_master", table = "policies")

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims
# MAGIC 
# MAGIC Claims records are read from the `insurance_demo_mongodb_master` schema and delivered to the DLT `staging` layer:

# COMMAND ----------

@dlt.table(
    name             = "staged_claims",
    comment          = "Staged claim records",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def stage_claims():
    # Stage the source claim records delivered to the Lakehouse by Fivetran
    return stage_records(schema = "insurance_demo_mongodb_master", table = "claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Traffic Accidents
# MAGIC 
# MAGIC Accident records are read from the `insurance_demo_s3` schema and delivered to the DLT `staging` layer:

# COMMAND ----------

@dlt.table(
    name             = "staged_accidents",
    comment          = "Staged accident records",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def stage_accidents():
    # Stage the source accident records delivered to the Lakehouse by Fivetran
    return stage_records(schema = "insurance_demo_s3", table = "accidents")

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------


