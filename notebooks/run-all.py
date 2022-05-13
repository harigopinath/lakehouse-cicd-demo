# Databricks notebook source
# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

#dbutils.widgets.removeAll()
dbutils.widgets.dropdown("clean_up", "no", ["yes", "no"], "Delete all existing data")
dbutils.widgets.text("BasePath", "/tmp/hari-lakehouse", "Base path of all data")
dbutils.widgets.text("DatabaseName", "hari_lakehouse_classic", "Name of the database")

# COMMAND ----------

base_path     = dbutils.widgets.get("BasePath")
database_name = dbutils.widgets.get("DatabaseName")
print(base_path)
print(database_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional clean-up

# COMMAND ----------

# Drop the main database
def drop_database(database_name: str):
    try:
        spark.sql(f"USE {database_name}")
    except Exception as e:
        return

    try:
        print(f"Getting all tables from '{database_name}' database")
        all_tables = list(map(lambda t: f"{database_name}.{t[0]}",
                              spark.sql(f"SHOW TABLES FROM {database_name}").select("tableName").collect()))
        for table in all_tables:
            print(f"Dropping table '{table}'")
            spark.sql(f"DROP TABLE {table}")
        print(f"Dropping database '{database_name}'")
        spark.sql(f"DROP DATABASE {database_name}")
    except Exception as e:
        dbutils.notebook.exit(f"ERROR: {e}")


if dbutils.widgets.get("clean_up") == "yes":
    drop_database(dbutils.widgets.get("DatabaseName"))
    print(f"Removing base path '{base_path}'")
    dbutils.fs.rm(base_path, True)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

dbutils.notebook.run("./setup", 1000, {"BasePath": base_path, "DatabaseName": database_name})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run all pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze pipelines

# COMMAND ----------

dbutils.notebook.run("./01-world-bank-bronze", 1000, {"BasePath": base_path, "DatabaseName": database_name})
dbutils.notebook.run("./01-overdoses-bronze", 1000, {"BasePath": base_path, "DatabaseName": database_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver pipelines

# COMMAND ----------

dbutils.notebook.run("./02-world-bank-silver", 1000, {"BasePath": base_path, "DatabaseName": database_name})
dbutils.notebook.run("./02-overdoses-silver", 1000, {"BasePath": base_path, "DatabaseName": database_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold pipelines

# COMMAND ----------

dbutils.notebook.run("./03-indicators-gold", 1000, {"BasePath": base_path, "DatabaseName": database_name})

# COMMAND ----------


