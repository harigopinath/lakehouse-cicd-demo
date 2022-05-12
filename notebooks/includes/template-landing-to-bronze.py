# Databricks notebook source
# MAGIC %md
# MAGIC #### Includes

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checks

# COMMAND ----------

print(landing_path)
print(bronze_table_path)
print(bronze_checkpoint_path)

# COMMAND ----------

display(dbutils.fs.ls(landing_path))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM csv.`{landing_path}` LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Landing to Bronze stream

# COMMAND ----------

# read the data stream and add the partition column
stream_df = fix_dataframe_columns(read_stream_csv(spark, landing_path)
                                  .withColumn("ingestion_date", F.current_timestamp().cast("date"))
                                 )

# COMMAND ----------

query_name = "LandingToBronze"

(stream_df.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", bronze_checkpoint_path)
  .queryName(query_name)
  .partitionBy("ingestion_date")
  .trigger(once=True)
#  .trigger(processingTime="2 minutes")                            # Configure for a 2-minutes micro-batch
  .start(bronze_table_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check results

# COMMAND ----------

wait_for_stream(spark, query_name)
display(spark.sql(f"SELECT * FROM delta.`{bronze_table_path}`").limit(5))

# COMMAND ----------

wait_for_stream(spark, query_name)
display(spark.sql(f"SELECT COUNT(*) FROM delta.`{bronze_table_path}`").limit(5))

# COMMAND ----------

wait_for_stream(spark, query_name)
display(dbutils.fs.ls(bronze_table_path))
display(dbutils.fs.ls(bronze_checkpoint_path))