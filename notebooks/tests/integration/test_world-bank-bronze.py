# Databricks notebook source
# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

#dbutils.widgets.removeAll()
dbutils.widgets.text("BasePath", "/tmp/test_lakehouse_classic", "Base path of all data")
dbutils.widgets.text("DatabaseName", "test_lakehouse_classic", "Name of the database")

# COMMAND ----------

base_path = dbutils.widgets.get("BasePath")
database_name = dbutils.widgets.get("DatabaseName")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

dbutils.fs.mkdirs(base_path)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set the configuration

# COMMAND ----------

# MAGIC %run ../../includes/configuration

# COMMAND ----------

print(landing_base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare test data

# COMMAND ----------

dbutils.fs.put(f"/tmp/test_scripts/download_wb_data.sh","""
#!/bin/bash
world_bank_location='""" + wb_landing_path + """'

mkdir -p ${world_bank_location} || exit 1
cd ${world_bank_location} || exit 1

[[ -f "indicators_usa.csv" ]] || wget -q "https://data.humdata.org/dataset/579c7dac-b607-4f3b-8cd4-c97b7c219843/resource/02d50c9b-2687-4805-8e79-aa179ff75ccf/download/indicators_usa.csv" -O "indicators_usa.csv"

mkdir -p "/dbfs${world_bank_location}"
cp -a ${world_bank_location}/* /dbfs${world_bank_location}
""", True)

# COMMAND ----------

# MAGIC %sh
# MAGIC /bin/bash /dbfs/tmp/test_scripts/download_wb_data.sh

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the notebook

# COMMAND ----------

dbutils.notebook.run("../../01-world-bank-bronze", 1000, {"BasePath": base_path, "DatabaseName": database_name})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the results

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the test results

# COMMAND ----------

dbutils.fs.ls(wb_bronze_table_path)

# COMMAND ----------

test_df = spark.read.format("delta").load(wb_bronze_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check row count

# COMMAND ----------

assert test_df.count() >= 70000

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check schema

# COMMAND ----------

assert 'Country_Name' in test_df.schema.names
assert 'Year' in test_df.schema.names
assert 'Value' in test_df.schema.names
assert 'Indicator_Name' in test_df.schema.names
assert 'Indicator_Code' in test_df.schema.names

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check data

# COMMAND ----------

from pyspark.sql import functions as F
value_to_test = test_df.filter(F.col("Indicator_Code") == "AG.AGR.TRAC.NO").filter(F.col("Year") == "1983").head()['Value']

assert str(value_to_test) == "4671000"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean-up

# COMMAND ----------

spark.sql(f"DROP DATABASE {database_name}")

# COMMAND ----------

dbutils.fs.rm(base_path, True)