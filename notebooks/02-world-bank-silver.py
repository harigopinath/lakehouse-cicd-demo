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

# MAGIC %run ./includes/transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run template notebook

# COMMAND ----------

print(descriptions_table_path)

# COMMAND ----------

bronze_table_path       = wb_bronze_table_path
silver_table_path       = wb_silver_table_path
silver_checkpoint_path  = wb_silver_checkpoint_path
descriptions_table_path = descriptions_table_path
process_bronze_func     = process_wb_bronze

# COMMAND ----------

# MAGIC %run ./includes/template-bronze-to-silver