# Databricks notebook source
# MAGIC %md
# MAGIC # Background
# MAGIC 
# MAGIC Curated policy, claims and accident records are aggregated to create business-level views that can be consumed for delivering automated reports and oversight of key performance metrics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Setup
# MAGIC 
# MAGIC The Delta Live Tables (DLT) pipeline is configured to read data from the `curated` layer and create an `aggregated` layer (also known as the `gold` layer).

# COMMAND ----------

# Load the required libraries
import dlt

from pyspark.sql import types as T
from pyspark.sql import functions as F

# COMMAND ----------

# Declare a global constant for the layer identifier
LAYER = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Sources

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims
# MAGIC 
# MAGIC Curated claims records are aggregated to create daily, weekly, and monthly-level views:

# COMMAND ----------

# Load the required libraries
from pyspark.sql.window import Window

# COMMAND ----------

@dlt.table(
    name             = "aggregated_claims_daily",
    comment          = "Aggregated view of daily claims",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect("valid_number_of_claims", "number_of_claims > 0")
def aggregate_claims_daily():
    # Read the curated claims record into memory
    curated_claims = dlt.read("curated_claims")
    
    # Specify the window configuration for rolling calculations
    w = Window.orderBy("claim_date")

    # Construct an aggregate view of claims received on a daily basis
    aggregated_claims = curated_claims \
        .groupBy(F.col("claim_date")) \
        .agg(
            # Calculate the total number of claims
            F.count(F.lit(1)).alias("number_of_claims"),
            # Calculate the cumulative amounts claimed
            F.sum(F.col("total_claim_amount")).alias("total_claim_amount"),
            F.sum(F.col("injury_claim_amount")).alias("injury_claim_amount"),
            F.sum(F.col("property_claim_amount")).alias("property_claim_amount"),
            F.sum(F.col("vehicle_claim_amount")).alias("vehicle_claim_amount"),
            # Calculate the average hour of the day when incidents occurred
            F.round(F.avg(F.col("incident_hour"))).alias("average_incident_hour"),
            # Calculate the average age of the driver
            F.round(F.avg(F.col("driver_age"))).alias("average_driver_age")
        ) \
        .withColumn(
            # Calculate the percentage change in total number of claims between days
            "pct_change_number_of_claims", F.round(((F.col("number_of_claims") - F.lag(F.col("number_of_claims")).over(w)) / F.lag(F.col("number_of_claims")).over(w)) * 100, 2)
        ) \
        .withColumn(
            # Calculate the 30-day rolling average for the total claims amount
            "30d_rolling_avg_total_claim_amount", F.round(F.avg(F.col("total_claim_amount")).over(w.rowsBetween(-30, 0)))
        ) \
        .orderBy(F.col("claim_date"))

    # Return the aggregated records
    return aggregated_claims

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

@dlt.table(
    name             = "aggregated_claims_weekly",
    comment          = "Aggregated view of weekly claims",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect("valid_number_of_claims", "number_of_claims > 0")
def aggregate_claims_weekly():
    # Read the curated claims record into memory
    curated_claims = dlt.read("curated_claims")
    
    # Specify the window configuration for rolling calculations
    w = Window.orderBy("claim_year_week")
    
    # Construct an aggregate view of claims received on a weekly basis
    aggregated_claims = curated_claims \
        .withColumn(
            # Extract the claim year/month information
            "claim_year_week", F.concat_ws("-", F.year(F.col("claim_date")).cast(T.StringType()), F.lpad(F.weekofyear(F.col("claim_date")).cast(T.StringType()), 2, "0"))
        ) \
        .groupBy(F.col("claim_year_week")) \
        .agg(
            # Calculate the total number of claims
            F.count(F.lit(1)).alias("number_of_claims"),
            # Calculate the cumulative amounts claimed
            F.sum(F.col("total_claim_amount")).alias("total_claim_amount"),
            F.sum(F.col("injury_claim_amount")).alias("injury_claim_amount"),
            F.sum(F.col("property_claim_amount")).alias("property_claim_amount"),
            F.sum(F.col("vehicle_claim_amount")).alias("vehicle_claim_amount"),
            # Calculate the average hour of the day when incidents occurred
            F.round(F.avg(F.col("incident_hour"))).alias("average_incident_hour"),
            # Calculate the average age of the driver
            F.round(F.avg(F.col("driver_age"))).alias("average_driver_age")
        ) \
        .withColumn(
            # Calculate the percentage change in total claims amount between days
            "pct_change_number_of_claims", F.round(((F.col("number_of_claims") - F.lag(F.col("number_of_claims")).over(w)) / F.lag(F.col("number_of_claims")).over(w)) * 100, 2)
        ) \
        .withColumn(
            # Calculate the 3-month rolling average for the total claims amount
            "3m_rolling_avg_total_claim_amount", F.round(F.avg(F.col("total_claim_amount")).over(w.rowsBetween(-3, 0)))
        ) \
        .orderBy(F.col("claim_year_week"))
    
    # Return the aggregated records
    return aggregated_claims

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

@dlt.table(
    name             = "aggregated_claims_monthly",
    comment          = "Aggregated view of monthly claims",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect("valid_number_of_claims", "number_of_claims > 0")
def aggregate_claims_monthly():
    # Read the curated claims record into memory
    curated_claims = dlt.read("curated_claims")
    
    # Specify the window configuration for rolling calculations
    w = Window.orderBy("claim_year_month")
    
    # Construct an aggregate view of claims received on a monthly basis
    aggregated_claims = curated_claims \
        .withColumn(
            # Extract the claim year/month information
            "claim_year_month", F.concat_ws("-", F.year(F.col("claim_date")).cast(T.StringType()), F.lpad(F.month(F.col("claim_date")).cast(T.StringType()), 2, "0"))
        ) \
        .groupBy(F.col("claim_year_month")) \
        .agg(
            # Calculate the total number of claims
            F.count(F.lit(1)).alias("number_of_claims"),
            # Calculate the cumulative amounts claimed
            F.sum(F.col("total_claim_amount")).alias("total_claim_amount"),
            F.sum(F.col("injury_claim_amount")).alias("injury_claim_amount"),
            F.sum(F.col("property_claim_amount")).alias("property_claim_amount"),
            F.sum(F.col("vehicle_claim_amount")).alias("vehicle_claim_amount"),
            # Calculate the average hour of the day when incidents occurred
            F.round(F.avg(F.col("incident_hour"))).alias("average_incident_hour"),
            # Calculate the average age of the driver
            F.round(F.avg(F.col("driver_age"))).alias("average_driver_age")
        ) \
        .withColumn(
            # Calculate the percentage change in total claims amount between days
            "pct_change_number_of_claims", F.round(((F.col("number_of_claims") - F.lag(F.col("number_of_claims")).over(w)) / F.lag(F.col("number_of_claims")).over(w)) * 100, 2)
        ) \
        .withColumn(
            # Calculate the 3-month rolling average for the total claims amount
            "3m_rolling_avg_total_claim_amount", F.round(F.avg(F.col("total_claim_amount")).over(w.rowsBetween(-3, 0)))
        ) \
        .orderBy(F.col("claim_year_month"))
    
    # Return the aggregated records
    return aggregated_claims

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accidents
# MAGIC 
# MAGIC Curated accident records are aggregated to create daily, weekly, and monthly-level views:

# COMMAND ----------

@dlt.table(
    name             = "aggregated_accidents_daily",
    comment          = "Aggregated view of daily accidents",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect("valid_number_of_accidents", "number_of_accidents > 0")
def aggregate_accidents_daily():
    # Read the curated accident and incident records into memory
    curated_accidents = dlt.read("curated_accidents")
    curated_incidents = dlt.read("curated_incidents")
    
    # Reformat the list of incident-set columns to match that of the accident records
    cols = [col.replace("incident", "accident") for col in curated_incidents.columns]
    # Create an updated view of the incident records with the renamed features
    curated_incidents = curated_incidents.toDF(*cols)

    # Specify the window configuration for rolling calculations
    w = Window.orderBy("accident_date")
    
    # Construct an aggregate view of daily accidents
    aggregated_accidents = curated_accidents \
        .select(*cols) \
        .union(curated_incidents) \
        .groupBy(F.col("accident_date")) \
        .agg(
            # Calculate the total number of accidents
            F.count(F.lit(1)).alias("number_of_accidents"),
            # Calculate the average hour of the day when accidents occurred
            F.round(F.avg(F.col("accident_hour"))).alias("average_accident_hour"),
            # Calculate the 75th percentile for the number of vehicles involved in an accident
            F.expr("percentile(number_of_vehicles_involved, array(0.75))")[0].alias("75_pctl_number_of_vehicles_involved"),
            # Get the most common areas where accidents occurred
            F.max(F.col("borough")).alias("most_common_borough"),
            F.max(F.col("zip_code")).alias("most_common_zip_code")
        ) \
        .withColumn(
            # Calculate the percentage change in total number of accidents between days
            "pct_change_number_of_accidents", F.round(((F.col("number_of_accidents") - F.lag(F.col("number_of_accidents")).over(w)) / F.lag(F.col("number_of_accidents")).over(w)) * 100, 2)
        ) \
        .withColumn(
            # Calculate the 30-day rolling average for the total number of accidents
            "30d_rolling_avg_number_of_accidents", F.round(F.avg(F.col("number_of_accidents")).over(w.rowsBetween(-30, 0)))
        ) \
        .orderBy(F.col("accident_date"))
    
    # Return the aggregated records
    return aggregated_accidents

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

@dlt.table(
    name             = "aggregated_accidents_weekly",
    comment          = "Aggregated view of weekly accidents",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect("valid_number_of_accidents", "number_of_accidents > 0")
def aggregate_accidents_weekly():
    # Read the curated accident and incident records into memory
    curated_accidents = dlt.read("curated_accidents")
    curated_incidents = dlt.read("curated_incidents")
    
    # Reformat the list of incident-set columns to match that of the accident records
    cols = [col.replace("incident", "accident") for col in curated_incidents.columns]
    # Create an updated view of the incident records with the renamed features
    curated_incidents = curated_incidents.toDF(*cols)

    # Specify the window configuration for rolling calculations
    w = Window.orderBy("accident_year_week")
    
    # Construct an aggregate view of weekly accidents
    aggregated_accidents = curated_accidents \
        .select(*cols) \
        .union(curated_incidents) \
        .withColumn(
            # Extract the accident year/week information
            "accident_year_week", F.concat_ws("-", F.year(F.col("accident_date")).cast(T.StringType()), F.lpad(F.weekofyear(F.col("accident_date")).cast(T.StringType()), 2, "0"))
        ) \
        .groupBy(F.col("accident_year_week")) \
        .agg(
            # Calculate the total number of accidents
            F.count(F.lit(1)).alias("number_of_accidents"),
            # Calculate the average hour of the day when accidents occurred
            F.round(F.avg(F.col("accident_hour"))).alias("average_accident_hour"),
            # Calculate the 75th percentile for the number of vehicles involved in an accident
            F.expr("percentile(number_of_vehicles_involved, array(0.75))")[0].alias("75_pctl_number_of_vehicles_involved"),
            # Get the most common areas where accidents occurred
            F.max(F.col("borough")).alias("most_common_borough"),
            F.max(F.col("zip_code")).alias("most_common_zip_code")
        ) \
        .withColumn(
            # Calculate the percentage change in total number of accidents between days
            "pct_change_number_of_accidents", F.round(((F.col("number_of_accidents") - F.lag(F.col("number_of_accidents")).over(w)) / F.lag(F.col("number_of_accidents")).over(w)) * 100, 2)
        ) \
        .withColumn(
            # Calculate the 3-month rolling average for the total number of accidents
            "3m_rolling_avg_number_of_accidents", F.round(F.avg(F.col("number_of_accidents")).over(w.rowsBetween(-3, 0)))
        ) \
        .orderBy(F.col("accident_year_week"))
    
    # Return the aggregated records
    return aggregated_accidents

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

@dlt.table(
    name             = "aggregated_accidents_monthly",
    comment          = "Aggregated view of monthly accidents",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect("valid_number_of_accidents", "number_of_accidents > 0")
def aggregate_accidents_monthly():
    # Read the curated accident and incident records into memory
    curated_accidents = dlt.read("curated_accidents")
    curated_incidents = dlt.read("curated_incidents")
    
    # Reformat the list of incident-set columns to match that of the accident records
    cols = [col.replace("incident", "accident") for col in curated_incidents.columns]
    # Create an updated view of the incident records with the renamed features
    curated_incidents = curated_incidents.toDF(*cols)

    # Specify the window configuration for rolling calculations
    w = Window.orderBy("accident_year_month")
    
    # Construct an aggregate view of monthly accidents
    aggregated_accidents = curated_accidents \
        .select(*cols) \
        .union(curated_incidents) \
        .withColumn(
            # Extract the accident year/month information
            "accident_year_month", F.concat_ws("-", F.year(F.col("accident_date")).cast(T.StringType()), F.lpad(F.month(F.col("accident_date")).cast(T.StringType()), 2, "0"))
        ) \
        .groupBy(F.col("accident_year_month")) \
        .agg(
            # Calculate the total number of accidents
            F.count(F.lit(1)).alias("number_of_accidents"),
            # Calculate the average hour of the day when accidents occurred
            F.round(F.avg(F.col("accident_hour"))).alias("average_accident_hour"),
            # Calculate the 75th percentile for the number of vehicles involved in an accident
            F.expr("percentile(number_of_vehicles_involved, array(0.75))")[0].alias("75_pctl_number_of_vehicles_involved"),
            # Get the most common areas where accidents occurred
            F.max(F.col("borough")).alias("most_common_borough"),
            F.max(F.col("zip_code")).alias("most_common_zip_code")
        ) \
        .withColumn(
            # Calculate the percentage change in total number of accidents between days
            "pct_change_number_of_accidents", F.round(((F.col("number_of_accidents") - F.lag(F.col("number_of_accidents")).over(w)) / F.lag(F.col("number_of_accidents")).over(w)) * 100, 2)
        ) \
        .withColumn(
            # Calculate the 3-month rolling average for the total number of accidents
            "3m_rolling_avg_number_of_accidents", F.round(F.avg(F.col("number_of_accidents")).over(w.rowsBetween(-3, 0)))
        ) \
        .orderBy(F.col("accident_year_month"))
    
    # Return the aggregated records
    return aggregated_accidents

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policies
# MAGIC 
# MAGIC Curated policies are aggregated to create a monthly-level view:

# COMMAND ----------

@dlt.table(
    name             = "aggregated_policies_monthly",
    comment          = "Aggregated view of monthly policies",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def aggregate_policies_monthly():
    # Read the curated policy records into memory
    curated_policies = dlt.read("curated_policies")
    
    # Construct an aggregate view of monthly policies
    aggregated_policies = curated_policies \
        .withColumn(
            # Extract the policy issue date year/month information
            "year_month", F.concat_ws("-", F.year(F.col("issue_date")).cast(T.StringType()), F.lpad(F.month(F.col("issue_date")).cast(T.StringType()), 2, "0"))
        ) \
        .withColumn(
            # Calculate the average age of vehicles at the date of issuing policies
            "issue_age_of_vehicle", F.year(F.col("issue_Date")) - F.col("model_year").cast(T.IntegerType())
        ) \
        .groupBy(F.col("year_month")) \
        .agg(
            # Calculate the total number of policies used in each month
            F.count(F.lit(1)).alias("policies_issued"),
            # Calculate the total exposure for each month
            F.round(F.sum(F.col("sum_insured"))).alias("exposure"),
            # Calculate the average age of vehicles at the date of issuing policies
            F.round(F.avg(F.col("issue_age_of_vehicle"))).alias("avg_issue_age_of_vehicle")
        ) \
        .alias("a") \
        .join(
            curated_policies \
                .withColumn(
                    # Extract the policy expiry date year/month information
                    "year_month", F.concat_ws("-", F.year(F.col("expiry_date")).cast(T.StringType()), F.lpad(F.month(F.col("expiry_date")).cast(T.StringType()), 2, "0"))
                ) \
                .groupBy(F.col("year_month")) \
                .agg(
                    # Calculate the total number of policies that expired in each month
                    F.count(F.lit(1)).alias("policies_expired")
                ) \
                .alias("b"),
            on  = F.col("a.year_month") == F.col("b.year_month"),
            how = "left"
        ) \
        .select(*["a.year_month", "a.policies_issued", "b.policies_expired", "a.exposure", "a.avg_issue_age_of_vehicle"]) \
        .fillna(0, ["policies_issued", "policies_expired"]) \
        .orderBy(F.col("year_month"))
    
    # Return the aggregated records
    return aggregated_policies

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------


