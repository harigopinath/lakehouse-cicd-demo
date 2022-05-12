# Databricks notebook source
# MAGIC %md
# MAGIC ## Our World In Data (Drug Use)
# MAGIC   - https://ourworldindata.org/drug-use
# MAGIC   - https://ourworldindata.org/grapher/deaths-substance-disorders?country=~OWID_WRL

# COMMAND ----------

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

# MAGIC %md
# MAGIC ### Checks

# COMMAND ----------

print(overdoses_landing_path)
print(overdoses_bronze_table_path)
print(overdoses_bronze_checkpoint_path)

# COMMAND ----------

display(dbutils.fs.ls(overdoses_landing_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Landing to Bronze stream

# COMMAND ----------

# set the schema that should be read
overdoses_schema = T.StructType([
    T.StructField("entityKey", T.StringType(), True),
    T.StructField("variables", T.StringType(), True)
])

# read the data stream and add the partition column
overdoses_stream_df = (read_stream_json(spark, overdoses_landing_path, overdoses_schema)
    .withColumn("ingestion_date", F.current_timestamp().cast("date"))
)

# COMMAND ----------

query_name = "LandingToBronze"

(overdoses_stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", overdoses_bronze_checkpoint_path)
    .queryName(query_name)
    .partitionBy("ingestion_date")
    .trigger(once=True)
    #.trigger(processingTime="2 minutes")                            # Configure for a 2-minutes micro-batch
    .start(overdoses_bronze_table_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check results

# COMMAND ----------

wait_for_stream(spark, query_name)
spark.sql(f"SELECT * FROM delta.`{overdoses_bronze_table_path}`").limit(5).show()

# COMMAND ----------

wait_for_stream(spark, query_name)
display(spark.sql(f"SELECT COUNT(*) FROM delta.`{overdoses_bronze_table_path}`").limit(5))

# COMMAND ----------

wait_for_stream(spark, query_name)
display(dbutils.fs.ls(overdoses_bronze_table_path))
display(dbutils.fs.ls(overdoses_bronze_checkpoint_path))