# Databricks notebook source
# MAGIC %md
# MAGIC ## World Bank Health Indicators (supplementary data)
# MAGIC   - the USA: https://data.humdata.org/dataset/world-bank-combined-indicators-for-united-states
# MAGIC   - similarly for other developed nations

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

# MAGIC %md
# MAGIC ### Run template notebook

# COMMAND ----------

landing_path           = wb_landing_path
bronze_table_path      = wb_bronze_table_path
bronze_checkpoint_path = wb_bronze_checkpoint_path

# COMMAND ----------

# MAGIC %run ./includes/template-landing-to-bronze