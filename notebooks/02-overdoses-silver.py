# Databricks notebook source
# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

#dbutils.widgets.removeAll()
dbutils.widgets.text("BasePath", "/tmp/alexandru-lakehouse", "Base path of all data")
dbutils.widgets.text("DatabaseName", "alexandru_lakehouse_classic", "Name of the database")

# COMMAND ----------

base_path     = dbutils.widgets.get("BasePath")
database_name = dbutils.widgets.get("DatabaseName")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Includes

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

# MAGIC %run ./includes/transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checks

# COMMAND ----------

print(overdoses_bronze_table_path)
print(overdoses_silver_table_path)
print(overdoses_silver_checkpoint_path)

# COMMAND ----------

spark.sql(f"SELECT * FROM delta.`{overdoses_bronze_table_path}`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze to Silver stream

# COMMAND ----------

overdoses_stream_df = read_stream_delta(spark, overdoses_bronze_table_path)

# COMMAND ----------

from pyspark.sql import DataFrame

def process_and_upsert(batch_df: DataFrame, batch_id: int):
    # Process the micro-batch DataFrame
    overdoses_transformed_df = process_overdoses_bronze(batch_df)

    # Register the micro batch dataframe as a view
    overdoses_transformed_df.createOrReplaceTempView("src")

    # Create an empty Delta Silver table if it doesn't exist
    init_delta_table(spark, schema=overdoses_transformed_df.schema, path=overdoses_silver_table_path)

    # Use the view name to apply MERGE
    # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
    batch_df._jdf.sparkSession().sql("""
    MERGE INTO delta.`""" + overdoses_silver_table_path + """` dest
    USING src
    ON src.Year = dest.Year and src.Country_Code = dest.Country_Code
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

query_name = "BronzeToSilver"
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

(overdoses_stream_df.writeStream
  .format("delta")
  .outputMode("update")
  .option("checkpointLocation", overdoses_silver_checkpoint_path)
  .queryName(query_name)
  .foreachBatch(process_and_upsert)
  .trigger(once=True)
#  .trigger(processingTime="2 minutes")                            # Configure for a 2-minutes micro-batch
  .start(overdoses_silver_table_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Register the Silver table

# COMMAND ----------

wait_for_stream(spark, query_name)
register_delta_table(spark, table_name=overdoses_silver_table_name, path=overdoses_silver_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check results

# COMMAND ----------

wait_for_stream(spark, query_name)
display(spark.sql("SELECT * FROM delta.`{}`".format(overdoses_silver_table_path)).limit(5))

# COMMAND ----------

wait_for_stream(spark, query_name)
display(spark.sql("SELECT COUNT(*) FROM delta.`{}`".format(overdoses_silver_table_path)).limit(5))

# COMMAND ----------

wait_for_stream(spark, query_name)
display(dbutils.fs.ls(overdoses_silver_table_path))
display(dbutils.fs.ls(overdoses_silver_checkpoint_path))