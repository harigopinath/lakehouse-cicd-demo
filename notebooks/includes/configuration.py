# Databricks notebook source
# MAGIC %md
# MAGIC ##### Paths

# COMMAND ----------

print("hello")

# COMMAND ----------

landing_base_path       = f"{base_path.rstrip('/')}/health-data-source"
pipeline_base_path      = f"{base_path.rstrip('/')}/classic/life-expectancy"

bronze_tables_base_path = f"{pipeline_base_path}/bronze"
silver_tables_base_path = f"{pipeline_base_path}/silver"
gold_tables_base_path   = f"{pipeline_base_path}/gold"

# COMMAND ----------

overdoses_landing_path           = f"{landing_base_path}/overdoses"
overdoses_bronze_table_path      = f"{bronze_tables_base_path}/overdoses.delta"
overdoses_bronze_checkpoint_path = f"{bronze_tables_base_path}/overdoses.checkpoint"
overdoses_silver_table_path      = f"{silver_tables_base_path}/overdoses.delta"
overdoses_silver_checkpoint_path = f"{silver_tables_base_path}/overdoses.checkpoint"

# COMMAND ----------

who_landing_path           = f"{landing_base_path}/who"
who_bronze_table_path      = f"{bronze_tables_base_path}/who_indicators.delta"
who_bronze_checkpoint_path = f"{bronze_tables_base_path}/who_indicators.checkpoint"
who_silver_table_path      = f"{silver_tables_base_path}/health_indicators.delta"
who_silver_checkpoint_path = f"{silver_tables_base_path}/who_indicators.checkpoint"

# COMMAND ----------

wb_landing_path           = f"{landing_base_path}/world_bank"
wb_bronze_table_path      = f"{bronze_tables_base_path}/world_bank_indicators.delta"
wb_bronze_checkpoint_path = f"{bronze_tables_base_path}/world_bank_indicators.checkpoint"
wb_silver_table_path      = f"{silver_tables_base_path}/health_indicators.delta"
wb_silver_checkpoint_path = f"{silver_tables_base_path}/world_bank_indicators.checkpoint"

# COMMAND ----------

health_indicators_silver_table_path = f"{silver_tables_base_path}/health_indicators.delta"
descriptions_table_path             = f"{silver_tables_base_path}/health_indicators_descriptions.delta"
health_indicators_gold_table_path   = f"{gold_tables_base_path}/health_indicators.delta"
all_indicators_gold_table_path      = f"{gold_tables_base_path}/all_indicators.delta"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Tables

# COMMAND ----------

descriptions_table_name      = "descriptions"
overdoses_silver_table_name  = "overdoses_silver"
indicators_silver_table_name = "silver_health_indicators"
descriptions_table_name      = "silver_health_indicators_descriptions"
indicators_gold_table_name   = "gold_health_indicators"
all_gold_table_name          = "gold_all_indicators"
