# Databricks notebook source
# DBTITLE 1,Ingesting our incoming data
import dlt
from pyspark.sql.functions import *
##Create the bronze information table containing the raw JSON data taken from the storage path printed in Cmd5 in 00_Retail_Data_CDC_Generator notebook
@dlt.create_table(name="customers_cdc",
                  comment = "New customer data incrementally ingested from cloud object storage landing zone")
def customers_cdc():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/demos/dlt/cdc_raw/customers"))

# COMMAND ----------

# DBTITLE 1,Silver Layer - Cleansed Table (Impose Constraints)
#This could also be a view: create_view
@dlt.create_table(name="customers_cdc_clean",
                  comment="Cleansed cdc data, tracking data quality with a view. We ensude valid JSON, id and operation type")
@dlt.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_operation", "operation IN ('APPEND', 'DELETE', 'UPDATE')")
def customers_cdc_clean():
  return dlt.read_stream("customers_cdc") \
            .select("address", "email", "id", "firstname", "lastname", "operation", "operation_date", "_rescued_data")

# COMMAND ----------

# DBTITLE 1,Create the target customers table 
dlt.create_target_table(name="customers", comment="Clean, materialized customers")

# COMMAND ----------

dlt.apply_changes(
  target = "customers", #The customer table being materilized
  source = "customers_cdc_clean", #the incoming CDC
  keys = ["id"], #what we'll be using to match the rows to upsert
  sequence_by = col("operation_date"), #we deduplicate by operation date getting the most recent value
  ignore_null_updates = False,
  apply_as_deletes = expr("operation = 'DELETE'"), #DELETE condition
  except_column_list = ["operation", "operation_date", "_rescued_data"]) #in addition we drop metadata columns

# COMMAND ----------

# DBTITLE 1,Slowly Changing Dimention of type 2 (SCD2)
#create the table
dlt.create_target_table(name="SCD2_customers", comment="Slowly Changing Dimension Type 2 for customers")

#store all changes as SCD2
dlt.apply_changes(
  target = "SCD2_customers", 
  source = "customers_cdc_clean",
  keys = ["id"], 
  sequence_by = col("operation_date"),
  ignore_null_updates = False,
  apply_as_deletes = expr("operation = 'DELETE'"), 
  except_column_list = ["operation", "operation_date", "_rescued_data"],
  stored_as_scd_type = "2") #Enable SCD2 and store individual updates
