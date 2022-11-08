# Databricks notebook source
# MAGIC %md
# MAGIC # Background
# MAGIC 
# MAGIC Data from policy, claim and accident sources is ingested using Fivetran and delivered to the Lakehouse. The DLT pipeline is used to read source records and create a `staging` (or `bronze`) layer. The `bronze` layer contains carbon copies of source records. 
# MAGIC 
# MAGIC Bronze-layer records are fed to through `curation` process of feature manipulation and engineering to create a set of transactional-level records.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Setup
# MAGIC 
# MAGIC The Delta Live Tables (DLT) pipeline is configured to read data from the `staging` layer and create a `curated` layer (also known as the `silver` layer).

# COMMAND ----------

# Load the required libraries
import dlt
import json

from pyspark.sql import types as T
from pyspark.sql import functions as F

# COMMAND ----------

# Declare a global constant for the layer identifier
LAYER = "silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Utility Methods
# MAGIC 
# MAGIC Data received from the claims application database comprised unstructured records with nested attributes. These attributes should be unpacked to create a flattened table structure that's easily queryable. The `get_dtype` method allows for easy inspection of predefined data schemas and facilitate unpacking of nested attributes:

# COMMAND ----------

# Load the required libraries
import re

from inspect import signature
from functools import partial
from itertools import product

# Load the classes required for type hinting
from typing import Optional, Union

# COMMAND ----------

def get_dtype(spec: Union[dict, str], evaluated: Optional[bool] = False, scope: Optional[str] = "*") -> Union[object, str]:
    """
    Returns a valid string representation or instantiated object of a Spark DataType from a field
    type specification.

    Parameters
    ----------
    spec: dict or str
        Collection or attributes containing the detailed configuration of the field type. Primitive
        types can be specified using a simple string.

    evaluated: bool, optional
        Parse and evaluate the formulated data-type string expression.

    scope: str, optional
        Specifies the scope where the PySpark Type classes can be accessed. The default scope
        assumes that all types were imported, i.e. using the following import statement:

        `from pyspark.sql.types import *`

        Use the name of the specified scope to indicate named imports, for example:

        `from pyspark.sql import types as T`

        Here, the value for the argument should be 'T'.

    Returns
    -------
    dtype: object or str
        String representation of the field Spark DataType constructor. Returns an instantiated object
        of the class if `evaluated` is set to True.

    Examples
    --------
    # For a primitive type like a field containing string-type values, a simple string specification
    # can be used:

    >>> get_dtype(spec = "string")
    'StringType()'

    # To get the instantiated class:

    >>> get_dtype(spec = "string", evaluated = True)
    StringType

    # For complex data types, use a collection of attributes:

    >>> get_dtype(spec = {"type": "array", "element": {"type": "string"}})
    'ArrayType(StringType())'

    # For fields containing structures, use the `data` attribute to indicate the child types:

    >>> get_dtype(spec = {"type": "struct", "fields": [{"name": "field_1", "data": "string"}, {"name": "field_2", "data": {"type": "map", "key": "string", "value": "integer"}}]})
    'StructType([StructField("field_1", StringType(), True), StructField("field_2", MapType(StringType(), IntegerType(), True), True)])'

    # To specify that a field value may not be null, use the `null` attribute:

    >>> get_dtype(spec = {"type": "struct", "fields": [{"name": "field_1", "data": "string", "null": False}]})
    'StructType([StructField("field_1", StringType(), False)])'
    """

    # Declare a constant with the list of known primitive Spark DataType classes
    PRIMITIVE_TYPES = ["binary", "boolean", "byte", "date", "decimal", "double", "float", "integer", "long", "short", "string", "timestamp"]

    # Prepare the passed scope for validation
    scope = scope.rstrip(".")
    # Check the scope for the PySpark Type classes
    scope = f"{scope}." if len(scope) > 0 and scope != "*" else ""

    # Validate the type specification
    if not isinstance(spec, (dict, str)):
        # Raise an exception error
        raise ValueError("Invalid argument received for 'spec'. Must be one of type 'dict' or 'str'.")

    # Validate the type of the specification
    if isinstance(spec, str):
        # Validate the specification against the list of known primitive types
        if spec not in PRIMITIVE_TYPES:
            # Raise an exception error
            raise ValueError(f"Invalid specification for Spark DataType. Primitive type must be one of {', '.join(PRIMITIVE_TYPES)}.")

        # Get the string representation of the type class
        dtype = f"{scope}{spec.title()}Type()"
        # Return the requested response
        return eval(dtype) if evaluated else dtype

    else:
        # Check the specification for signs of a StructField class instruction
        if "data" in spec:
            # Validate the specification
            if not all(key in spec for key in ["name", "data"]):
                # Raise an exception error
                raise KeyError(f"Missing attribute(s) in 'spec'. StructField type specification must include reference to 'name' and 'data' attributes.")

            # Determine whether the field can contain null values
            nullable = True if "null" not in spec else spec.get("null")

            # Get the string representation of the type class
            dtype = f"{scope}StructField(\"{spec.get('name')}\", {get_dtype(spec = spec.get('data'), scope = scope)}, {nullable})"
            # Return the requested response
            return eval(dtype) if evaluated else dtype

        # Validate the structure of the specification
        if "type" not in spec:
            # Raise an exception error
            raise KeyError("Missing attribute in 'spec'. Type specification must include reference for 'type' attribute.")

        # Extract the type specification
        _type = spec.get("type")
        # Check if the specification contains any additional configuration details
        if len(set(spec.keys()) - {"name", "type"}) > 0:
            # Get the signature of the type class
            sig = signature(eval(f"{scope}{_type.title()}Type"))
            # Extract the class parameters
            params = [*sig.parameters]

            # Compile a regular expression pattern to identify additional type class attributes
            pattern = re.compile(r"type$|.*(?<!null)$", re.IGNORECASE)
            # Filter the list of class parameters for the desired subset
            params = [re.sub("type", "", param, flags = re.IGNORECASE) for param in params if pattern.search(param)]
            # Validate the search result
            if not params:
                # Raise an exception error
                raise ValueError("Failed to identify required attributes from type class inspection.")

            # Validate the type specification
            if not all(param in [*spec] for param in params):
                # Raise an exception error
                raise KeyError(f"Missing attribute(s) in 'spec'. Complex type specification '{_type}' must include reference to all of {', '.join(params)}.")

            # Initialise an empty list to store the set of parameter types
            elements = []
            # Iterate over the list of identified parameters
            for param in params:
                # Get the current parameter specification
                _spec = spec.get(param)
                # Validate the data type of the parameter specification
                if isinstance(_spec, list):
                    # Recursively evaluate the types for each of the parameter specifications
                    elements.append(f"[{', '.join(map(partial(get_dtype, scope = scope), _spec))}]")

                else:
                    # Recursively evaluate the parameter specification
                    elements.append(get_dtype(spec = _spec, scope = scope))

            # Get the string representation of the type class
            dtype = f"{scope}{_type.title()}Type({', '.join(elements)})"
            # Return the requested response
            return eval(dtype) if evaluated else dtype

        else:
            # Recursively call the method with the extracted type specification
            return get_dtype(spec = _type, evaluated = evaluated, scope = scope)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Sources

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accidents
# MAGIC 
# MAGIC Traffic accident records are parsed to create additional (custom) features, e.g. the number of vehicles involved in an accident can be derived from a subset of features included in the original dataset.
# MAGIC 
# MAGIC The `curated_accidents` table is configured to apply a set of predefined data quality metrics using the `expect_all_or_drop` decorator. Using this approach, DLT can enforce granular quality controls, for example to check whether the reported accident date represents a date in the future.

# COMMAND ----------

# Open a connection to the predefined data model
with open("/dbfs/e2-demo-gtm-insurance/schemas/s3/accidents.json", "r") as f:
    # Load the contents of the data model
    schema_accidents = json.load(f)

# COMMAND ----------

@dlt.table(
    name             = "curated_accidents",
    comment          = "Curated vehicle accident records",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all_or_drop({
    "valid_accident_date": "accident_date < current_date()",
    "valid_accident_hour": "accident_hour between 0 and 24",
    "valid_borough": "borough IS NOT NULL",
    "valid_zip_code": "zip_code IS NOT NULL",
    "valid_latitude": "latitude IS NOT NULL",
    "valid_longitude": "longitude IS NOT NULL"
})
def curate_accidents():
    # Read the staged accident records into memory
    staged_accidents = dlt.read("staged_accidents")
    
    # Iterate over the specified schema
    for spec in schema_accidents:
        # Get the name for the current feature
        col_name = spec.get("name")
        
        # Get the data type for the current feature
        dtype = get_dtype(spec = spec, evaluated = True, scope = "T")
        # Update the current feature data type
        staged_accidents = staged_accidents.withColumn(col_name, F.col(col_name).cast(dtype))    
    
    # Instantiate a variable to store the list of personal-injury related conditions
    conditions_personal_injury = []
    # Iterate over the set of all possible personal-injury indicators
    for (a, b) in list(product(["persons", "pedestrians", "cyclist", "motorist"], ["injured", "killed"])):
        # Record the current condition statement
        conditions_personal_injury.append(f"(F.col('number_of_{a}_{b}') > 0)")
        
    # Construct a single conditional expression
    condition_personal_injury = " | ".join(conditions_personal_injury)
    
    # Update the record features
    curated_accidents = staged_accidents \
        .withColumn(
            # Parse the accident date/time information to retian only the date values
            "accident_date", F.to_date(F.col("accident_date"))
        ) \
        .withColumn(
            # Extract the hour of the day that the accident occurred
            "accident_hour", F.split(F.col("accident_time"), ":").getItem(0)
        ) \
        .withColumn(
            # Check if any injury to a person (or persons) occurred
            "injury_to_person", F.when(eval(condition_personal_injury), F.lit(1)).otherwise(F.lit(1))
        ) \
        .withColumn(
            # Construct an array from the values of all type codes for vehicles involved in an accident
            "vehicle_type_codes", F.array(*[F.col(f"vehicle_type_code_{i}") for i in range(1, 6)])
        ) \
        .withColumn(
            # Calculate the total number of vehicles involved in an accident
            "number_of_vehicles_involved", F.expr("size(filter(vehicle_type_codes, x -> x is not null))")
        ) \
        .drop(*["accident_time", "vehicle_type_codes"])
    
    # Get the list of frame column names
    cols = curated_accidents.columns
    # Reorder the column names
    cols.insert(1, cols.pop(-1))
    
    # Return the curated dataset
    return curated_accidents.select(*cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims
# MAGIC 
# MAGIC Staged claims records are parsed to unpack nested attributes and create a set of additional (custom) features and apply a set of predefined data quality metrics.

# COMMAND ----------

# Load the classes required for type hinting
from pyspark.sql import DataFrame

# COMMAND ----------

def unpack_nested(df: DataFrame, schema: dict) -> DataFrame:
    """
    Unpack nested attributes from StructType columns to create a flat table structure.
    
    Parameters
    ----------
    df: pyspark.sql.DataFrame
        Original set of records.
    
    schema: dict
        Collection of attributes representing the data model of the source dataset.
        
    Returns
    -------
    df: pyspark.sql.DataFrame
        Flattened set of records with all nested attributes unpacked.
    """
    
    # Iterate over the source schema specification
    for spec in schema:
        # Check the current feature specification for nested attributes
        if spec.get("type") not in ("struct"):
            # Get the alias specification for the current feature
            alias = spec.get("alias")
            # Validate the feature alias
            if alias is not None:
                # Rename the source feature
                df = df.withColumnRenamed(spec.get("name"), alias)
            
            # Skip to the next feature specification
            continue
        
        # Evaluate the current feature specification schema
        s = get_dtype(spec = spec, evaluated = True, scope = "T")

        # Get the name of the parent column
        col = spec.get("name")
        # Parse the nested set of features to create a queryable entity
        df = df.withColumn("_", F.from_json(F.col(col), schema = s))
        # Iterate over the set of nested features
        for field in spec.get("fields"):
            # Get the name of the source field
            field_name = field.get("name")
            # Evaluate the name of the resulting column
            col_name = field.get("alias", f"{col}_{field_name}")
            # Extract the current feature to the top level
            df = df.withColumn(col_name, F.col(f"_.{field_name}"))
            
        # Remove the source and temporary features
        df = df.drop(*[col, "_"])
        
    # Return the flattened set of records
    return df

# COMMAND ----------

# Open a connection to the predefined data model
with open("/dbfs/e2-demo-gtm-insurance/schemas/mongodb/claims.json", "r") as f:
    # Load the contents of the data model
    schema_claims = json.load(f)

# COMMAND ----------

@dlt.table(
    name             = "curated_claims",
    comment          = "Curated claim records",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all({
    "valid_driver_license": "driver_license_issue_date > (current_date() - cast(cast(driver_age AS INT) AS INTERVAL YEAR))",
    "valid_claim_amount": "total_claim_amount > 0",
    "valid_coverage": "months_since_covered > 0",
    "valid_incident_before_claim": "days_between_incident_and_claim > 0"
})
@dlt.expect_all_or_drop({
    "valid_claim_number": "claim_number IS NOT NULL",
    "valid_policy_number": "policy_number IS NOT NULL",
    "valid_claim_date": "claim_date < current_date()",
    "valid_incident_date": "incident_date < current_date()",
    "valid_incident_hour": "incident_hour between 0 and 24",
    "valid_driver_age": "driver_age > 16",
    "valid_effective_date": "policy_effective_date < current_date()",
    "valid_expiry_date": "policy_expiry_date <= current_date()"
})
def curate_claims():
    # Read the staged claim records into memory
    staged_claims = dlt.read("staged_claims")
    # Unpack all nested attributes to create a flattened table structure
    curated_claims = unpack_nested(df = staged_claims, schema = schema_claims)
    
    # Update the format of all date/time features
    curated_claims = curated_claims \
        .withColumn(
            # Reformat the claim date values
            "claim_date", F.to_date(F.col("claim_datetime"))
        ) \
        .withColumn(
            # Reformat the incident date values
            "incident_date", F.to_date(F.col("incident_date"), "dd-MM-yyyy")
        ) \
        .withColumn(
            # Reformat the driver license issue date values
            "driver_license_issue_date", F.to_date(F.col("driver_license_issue_date"), "dd-MM-yyyy")
        ) \
        .drop("claim_datetime")
    
    # Read the staged claim records into memory
    curated_policies = dlt.read("curated_policies")
    # Evaluate the validity of the claim
    curated_claims = curated_claims \
        .alias("a") \
        .join(
            curated_policies.alias("b"),
            on  = F.col("a.policy_number") == F.col("b.policy_number"),
            how = "left"
        ) \
        .select([F.col(f"a.{c}") for c in curated_claims.columns] + [F.col(f"b.{c}").alias(f"policy_{c}")  for c in ("effective_date", "expiry_date")]) \
        .withColumn(
            # Calculate the number of months between coverage starting and the claim being filed
            "months_since_covered", F.round(F.months_between(F.col("claim_date"), F.col("policy_effective_date")))
        ) \
        .withColumn(
            # Check if the claim was filed before the policy came into effect
            "claim_before_covered", F.when(F.col("claim_date") < F.col("policy_effective_date"), F.lit(1)).otherwise(F.lit(0))
        ) \
        .withColumn(
            # Calculate the number of days between the incident occurring and the claim being filed
            "days_between_incident_and_claim", F.datediff(F.col("claim_date"), F.col("incident_date"))
        )
    
    # Return the curated dataset
    return curated_claims

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incidents
# MAGIC 
# MAGIC Claimed-for incidents represent additional traffic accident information. The `curated` claims records are interrogated to extract additional information about traffic accidents that can be joined with the external accident dataset in downstream processes.

# COMMAND ----------

@dlt.table(
    name             = "curated_incidents",
    comment          = "Curated claim incident records",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all_or_drop({
    "valid_incident_date": "incident_date < current_date()",
    "valid_incident_hour": "incident_hour between 0 and 24",
    "valid_borough": "borough IS NOT NULL",
    "valid_zip_code": "zip_code IS NOT NULL"
})
def curate_incidents():
    # Read the curated claims records
    curated_claims = dlt.read("curated_claims")
    # Read the curated policy records
    curated_policies = dlt.read("curated_policies")
    
    # Select the subset of features pertaining to the claim incident
    curated_incidents = curated_claims \
        .alias("a") \
        .join(
            curated_policies.alias("b"),
            on  = F.col("a.policy_number") == F.col("b.policy_number"),
            how = "left"
        ) \
        .select(*[F.col(f"a.{c}") for c in ("incident_date", "incident_hour", "number_of_vehicles_involved", "injury_claim_amount")] + [F.col("b.borough"), F.col("b.zip_code")]) \
        .withColumn(
            # Check if any injury to a person (or persons) occurred
            "injury_to_person", F.when(F.col("injury_claim_amount") > 0, F.lit(1)).otherwise(F.lit(1))
        ) \
        .drop("injury_claim_amount")
    
    # Return the curated dataset
    return curated_incidents

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policies
# MAGIC 
# MAGIC Staged policy records are parsed and checked before creating a `curated` set of records for downstream processing:

# COMMAND ----------

# Open a connection to the predefined data model
with open("/dbfs/e2-demo-gtm-insurance/schemas/sql/policies.json", "r") as f:
    # Load the contents of the data model
    schema_policies = json.load(f)

# COMMAND ----------

@dlt.table(
    name             = "curated_policies",
    comment          = "Curated policy records",
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect("valid_sum_insured", "sum_insured > 0")
@dlt.expect_all_or_drop({
    "valid_policy_number": "policy_number IS NOT NULL",
    "valid_premium": "premium > 1",
    "valid_issue_date": "issue_date < current_date()",
    "valid_effective_date": "effective_date < current_date()",
    "valid_expiry_date": "expiry_date <= current_date()",
    "valid_model_year": "model_year > 0"
})
def curate_policies():
    # Read the staged policy records into memory
    staged_policies = dlt.read("staged_policies")
    
    # Iterate over the source schema specification
    for spec in schema_policies:
        # Get the name for the current feature
        col = spec.get("name")
        
        # Check the type specification for the current feature
        if spec.get("type") in ("date"):
            # Reformat the current feature date values
            staged_policies = staged_policies.withColumn(col, F.to_date(F.col(col), spec.get("format")))
        
        # Get the alias specification for the current feature
        alias = spec.get("alias")
        # Validate the feature alias
        if alias is not None:
            # Rename the source feature
            staged_policies = staged_policies.withColumnRenamed(col, alias)

    # Update the policy premium values
    curated_policies = staged_policies.withColumn("premium", F.abs(F.col("premium")))
    
    # Return the curated dataset
    return curated_policies

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Reference Data
# MAGIC 
# MAGIC Accurate reference data forms a vital part of delivering high-quality data-driven applications. The policy and accident datasets represent opportunities to extract crucial reference data for geographical areas and motor vehicles.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geographical Areas
# MAGIC 
# MAGIC The external accident data source include high-quality information about geographical areas where accidents occurred. The `borough`, `neighborhood` and `zip_code` attributes can be extracted to create a contained reference dataset.

# COMMAND ----------

@dlt.table(
    name             = "curated_areas",
    comment          = "Curated set of distinct geographical area records",
    partition_cols   = [
        "borough"
    ],
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect("valid_neighborhood", "neighborhood IS NOT NULL")
@dlt.expect_all_or_drop({
    "valid_borough": "borough IS NOT NULL",
    "valid_zip_code": "zip_code IS NOT NULL"
})
def curate_areas():
    # Read the staged policy and accident records
    staged_policies  = dlt.read("staged_policies")
    staged_accidents = dlt.read("staged_accidents")

    # Extract the set of area-specific attributes from the policy records
    policy_areas = staged_policies \
        .select(*["borough", "neighborhood", "zip_code"]) \
        .dropDuplicates()
    
    # Extract and enrich the area-specific attributes from the accident records
    accident_areas = staged_accidents \
        .select(*["borough", "zip_code"]) \
        .dropDuplicates() \
        .alias("a") \
        .join(
            policy_areas.alias("b"),
            on  = (F.col("a.borough") == F.col("b.borough")) & (F.col("a.zip_code") == F.col("b.zip_code")),
            how = "left"
        ) \
        .select(*["a.borough", "b.neighborhood", "a.zip_code"])
    
    # Merge the two datasets and remove all duplicate records
    curated_areas = policy_areas\
        .union(accident_areas) \
        .dropDuplicates() \
        .orderBy(*["borough", "neighborhood"])
    
    # Return the curated dataset
    return curated_areas

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Motor Vehicles
# MAGIC 
# MAGIC The policy dataset includes detailed information about a large collection of vehicle makes, models and body types. While not exhaustive, the records provide a good starting point for creating a comprehensive reference dataset:

# COMMAND ----------

@dlt.table(
    name             = "curated_vehicles",
    comment          = "Curated set of distinct motor vehicle records",
    partition_cols   = [
        "make"
    ],
    table_properties = {
        "layer": LAYER,
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all_or_drop({
    "valid_make": "make IS NOT NULL and length(make) > 1",
    "valid_model": "model IS NOT NULL and length(model) > 1",
    "valid_body": "body IS NOT NULL and length(body) > 1"
})
def curate_vehicles():
    # Specify the set of desired columns
    cols = ["make", "model", "body"]
    
    # Return the set of curated vehicle records
    return dlt \
        .read("staged_policies") \
        .select(*cols) \
        .dropDuplicates() \
        .orderBy(*cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------


