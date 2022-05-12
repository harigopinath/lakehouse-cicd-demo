import pytest
import os

from pyspark.sql import types as T
from delta.tables import *

from lakehouseLibs import utils


@pytest.fixture(scope="session")
def spark_init():
    spark, logger = utils.spark_init(app_name="Spark_Test_App")
    yield spark, logger
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        spark.stop()


@pytest.fixture(scope="session")
def delta_init(spark_init):
    table_path = "/tmp/test_delta_table"
    spark, logger = spark_init
    schema = T.StructType([T.StructField("id", T.IntegerType(), True)])
    utils.init_delta_table(spark=spark, schema=schema, path=table_path)

    yield table_path
    import shutil
    shutil.rmtree(table_path, ignore_errors=True)


def test_fix_columns():
    """Test fix_columns"""
    #
    # Set all variables
    #
    column1 = "myColumn"
    column2 = "my Column"
    column3 = "my(Column)"

    #
    # Run the assertions
    #
    assert utils.fix_columns(column1) == "myColumn"
    assert utils.fix_columns(column2) == "my_Column"
    assert utils.fix_columns(column3) == "myColumn"


def test_fix_dataframe_columns(spark_init):
    """Test fix_dataframe_columns"""

    spark, logger = spark_init

    #
    # Set test data
    #
    test_data = [(1, "val1", "val11"), (2, "val2", "val22")]
    test_df = spark.createDataFrame(test_data, ["(c0", "_c1", "c 2"])
    expected_df = spark.createDataFrame(test_data, ["c0", "_c1", "c_2"])

    #
    # Run the assertions
    #
    fixed_df = utils.fix_dataframe_columns(test_df)
    assert fixed_df.columns == expected_df.columns


def test_init_delta_table(spark_init, delta_init):
    """Test init_delta_table"""

    spark, logger = spark_init
    table_path = delta_init

    assert DeltaTable.isDeltaTable(spark, table_path) is True


def test_register_delta_table(spark_init, delta_init):
    """Test register_delta_table"""

    spark, logger = spark_init
    table_path = delta_init
    table_name = "test_register_delta_table"

    utils.register_delta_table(spark=spark, table_name=table_name, path=table_path)

    all_tables = list(map(lambda r: r[0], spark.sql("SHOW TABLES").select("tableName").collect()))
    assert table_name in all_tables

    spark.sql(f"DROP TABLE {table_name}")
