# Databricks notebook source
# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

#dbutils.widgets.removeAll()
dbutils.widgets.text("BasePath", "/tmp/test_template_landing_to_bronze", "Base path of all data")

# COMMAND ----------

base_path = dbutils.widgets.get("BasePath")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

dbutils.fs.mkdirs(base_path)
landing_base_path = f"{base_path.rstrip('/')}/data-source"
print(landing_base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare test data

# COMMAND ----------

dbutils.fs.put(f"{landing_base_path}/file1.csv","""
col1,col2,col3
1,2,3
one,two,three
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the notebook

# COMMAND ----------

landing_path           = landing_base_path
bronze_table_path      = f"{base_path}/test_bronze.delta"
bronze_checkpoint_path = f"{base_path}/test_bronze.checkpoint"

# COMMAND ----------

# MAGIC %run ../../includes/template-landing-to-bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the results

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the test results

# COMMAND ----------

test_df = spark.read.format("delta").load(bronze_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check row count

# COMMAND ----------

assert test_df.count() == 2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check schema

# COMMAND ----------

assert 'col1' in test_df.schema.names
assert 'col2' in test_df.schema.names
assert 'col3' in test_df.schema.names

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check data

# COMMAND ----------

from pyspark.sql import functions as F
value_to_test = test_df.filter(F.col("col1") == "1").head()['col1']

assert str(value_to_test) == "1"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean-up

# COMMAND ----------

dbutils.fs.rm(base_path, True)