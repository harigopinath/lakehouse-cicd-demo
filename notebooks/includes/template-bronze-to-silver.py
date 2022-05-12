# Databricks notebook source
# MAGIC %md
# MAGIC #### Includes

# COMMAND ----------

# MAGIC %run ../includes/common

# COMMAND ----------

from delta.tables import DeltaTable
from delta.exceptions import ConcurrentAppendException

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.session import SparkSession

from lakehouseLibs import utils

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checks

# COMMAND ----------

print(bronze_table_path)
print(silver_table_path)
print(silver_checkpoint_path)
print(descriptions_table_path)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{bronze_table_path}`").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze to Silver stream

# COMMAND ----------

stream_df = read_stream_delta(spark, bronze_table_path)

# COMMAND ----------

def process_and_upsert(batch_df: DataFrame, batch_id: int):
    # Process the micro-batch DataFrame
    transformed_df = process_bronze_func(batch_df)

    # Extract the required columns for the Health Indicators Silver table
    df = transformed_df.select("Indicator", "Country", "Year", "Value")

    # Create an empty Delta Silver table if one doesn't exist
    init_delta_table(spark, schema=df.schema, path=silver_table_path, partition_columns=["Year"])

    # Register the dataframe as a view called src to be used in the MERGE statement
    df.createOrReplaceTempView("src")

    # Perform the MERGE using SQL syntax
    # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
    sql_merge = """
        MERGE INTO delta.`""" + silver_table_path + """` dest
        USING src
        ON src.Indicator = dest.Indicator and src.Year = dest.Year and src.Country = dest.Country
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    try:
        batch_df._jdf.sparkSession().sql(sql_merge)
    except ConcurrentAppendException as e:
        import time
        time.sleep(30)  # sleep 30s
        batch_df._jdf.sparkSession().sql(sql_merge)  # retry the Merge

    # Extract the required columns for the Descriptions table
    descriptions_df = transformed_df.select("Indicator", "Description").distinct()

    # Create an empty Delta Descriptions table if one doesn't exist
    init_delta_table(spark, schema=descriptions_df.schema, path=descriptions_table_path)

    # Get the Delta table object
    delta_table = DeltaTable.forPath(spark, descriptions_table_path)

    # Perform the MERGE using Python syntax
    merge_stmt = (delta_table.alias("dest").merge(descriptions_df.alias("src"), "src.Indicator = dest.Indicator")
                  .whenNotMatchedInsertAll()
                  .whenMatchedUpdateAll()
                  )
    try:
        merge_stmt.execute()
    except ConcurrentAppendException as e:
        import time
        time.sleep(30)  # sleep 30s
        merge_stmt.execute()  # retry the Merge

# COMMAND ----------

query_name = "BronzeToSilver"
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

(stream_df.writeStream
 .format("delta")
 .outputMode("update")
 .option("checkpointLocation", silver_checkpoint_path)
 .queryName(query_name)
 .foreachBatch(process_and_upsert)
 .trigger(once=True)
 #.trigger(processingTime="2 minutes")                            # Configure for a 2-minutes micro-batch
 .start()
 )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Register the Silver tables

# COMMAND ----------

wait_for_stream(spark, query_name)
register_delta_table(spark, table_name=indicators_silver_table_name, path=silver_table_path)
register_delta_table(spark, table_name=descriptions_table_name, path=descriptions_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check results

# COMMAND ----------

wait_for_stream(spark, query_name)
display(spark.sql(f"SELECT * FROM delta.`{silver_table_path}`").limit(5))

# COMMAND ----------

wait_for_stream(spark, query_name)
display(spark.sql(f"SELECT * FROM delta.`{descriptions_table_path}`").limit(5))

# COMMAND ----------

wait_for_stream(spark, query_name)
display(spark.sql(f"SELECT COUNT(*) FROM delta.`{silver_table_path}`").limit(5))

# COMMAND ----------

wait_for_stream(spark, query_name)
display(dbutils.fs.ls(silver_table_path))
display(dbutils.fs.ls(silver_checkpoint_path))
display(dbutils.fs.ls(descriptions_table_path))
