# Databricks notebook source
# MAGIC %md
# MAGIC ### Import the functions

# COMMAND ----------

# MAGIC %run ../../includes/common

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test fix_columns()

# COMMAND ----------

def test_fix_columns():
    #
    # Set all variables
    #
    column1 = "myColumn"
    column2 = "my Column"
    column3 = "my(Column)"

    #
    # Run the assertions
    #
    assert fix_columns(column1) == "myColumn"
    assert fix_columns(column2) == "my_Column"
    assert fix_columns(column3) == "myColumn"

test_fix_columns()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test fix_dataframe_columns()

# COMMAND ----------

def test_fix_dataframe_columns():
    #
    # Set test data
    #
    test_data = [(1, "val1", "val11"), (2, "val2", "val22")]
    test_df = spark.createDataFrame(test_data, ["(c0", "_c1", "c 2"])
    expected_df = spark.createDataFrame(test_data, ["c0", "_c1", "c_2"])

    #
    # Run the assertions
    #
    fixed_df = fix_dataframe_columns(test_df)
    assert fixed_df.columns == expected_df.columns

test_fix_dataframe_columns()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test init_delta_table()

# COMMAND ----------

def test_init_delta_table():
    table_path = "/tmp/test_init_delta_table"
    schema = T.StructType([T.StructField("id", T.IntegerType(), True)])
    
    init_delta_table(spark=spark, schema=schema, path=table_path)
    assert DeltaTable.isDeltaTable(spark, table_path) is True
    
    dbutils.fs.rm(table_path, True)

test_init_delta_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test register_delta_table()

# COMMAND ----------

def test_register_delta_table():
    table_name = "test_register_delta_table"
    table_path = "/tmp/test_register_delta_table"
    schema = T.StructType([T.StructField("id", T.IntegerType(), True)])
    init_delta_table(spark=spark, schema=schema, path=table_path)

    register_delta_table(spark, table_name, table_path)
    all_tables = list(map(lambda r: r[0], spark.sql("SHOW TABLES").select("tableName").collect()))
    assert table_name in all_tables

    spark.sql(f"DROP TABLE {table_name}")
    dbutils.fs.rm(table_path, True)

test_register_delta_table()