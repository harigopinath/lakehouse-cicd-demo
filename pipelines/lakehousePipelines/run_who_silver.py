import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from delta.exceptions import ConcurrentAppendException

from lakehouseLibs import utils
from lakehouseLibs import transformations


def set_paths(base_path: str) -> dict:
    pipeline_base_path = f"{base_path.rstrip('/')}/classic/life-expectancy"

    bronze_tables_base_path = f"{pipeline_base_path}/bronze"
    silver_tables_base_path = f"{pipeline_base_path}/silver"

    bronze_table_path = f"{bronze_tables_base_path}/who_indicators.delta"
    silver_table_path = f"{silver_tables_base_path}/health_indicators.delta"
    silver_checkpoint_path = f"{silver_tables_base_path}/who_indicators.checkpoint"
    descriptions_table_path = f"{silver_tables_base_path}/health_indicators_descriptions.delta"
    return {
        "bronze_table_path": bronze_table_path,
        "silver_table_path": silver_table_path,
        "silver_checkpoint_path": silver_checkpoint_path,
        "descriptions_table_path": descriptions_table_path
    }


def who_bronze_to_silver(spark: SparkSession, base_path: str, database_name: str) -> None:
    paths = set_paths(base_path)
    indicators_silver_table_name = f"{database_name}.silver_health_indicators"
    descriptions_table_name = f"{database_name}.silver_health_indicators_descriptions"

    #
    # Checks
    #
    print(paths)
    spark.sql(f"SELECT * FROM delta.`{paths['bronze_table_path']}` LIMIT 5").show()

    #
    # Bronze to Silver stream
    #
    # read the bronze delta stream
    stream_df = utils.read_stream_delta(spark, paths["bronze_table_path"])

    # define the forEachBatch function
    def process_and_upsert(batch_df: DataFrame, batch_id: int):
        # Process the micro-batch DataFrame
        transformed_df = transformations.process_who_bronze(batch_df)

        # Extract the required columns for the Health Indicators Silver table
        df = transformed_df.select("Indicator", "Country", "Year", "Value")

        # Create an empty Delta Silver table if one doesn't exist
        utils.init_delta_table(spark, schema=df.schema, path=paths['silver_table_path'], partition_columns=["Year"])

        # Register the dataframe as a view called src to be used in the MERGE statement
        df.createOrReplaceTempView("src")

        # Perform the MERGE using SQL syntax
        # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
        sql_merge = """
            MERGE INTO delta.`""" + paths['silver_table_path'] + """` dest
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
        utils.init_delta_table(spark, schema=descriptions_df.schema, path=paths['descriptions_table_path'])

        # Get the Delta table object
        delta_table = DeltaTable.forPath(spark, paths['descriptions_table_path'])

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

    query_name = "BronzeToSilver"
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

    (stream_df
     .writeStream
     .format("delta")
     .outputMode("update")
     .option("checkpointLocation", paths["silver_checkpoint_path"])
     .queryName(query_name)
     .foreachBatch(process_and_upsert)
     .trigger(once=True)
     #  .trigger(processingTime="2 minutes")                            # Configure for a 2-minutes micro-batch
     .start()
     )

    #
    # Register the Silver tables
    #
    utils.wait_for_stream(spark, query_name)
    utils.register_delta_table(spark, table_name=indicators_silver_table_name, path=paths['silver_table_path'])
    utils.register_delta_table(spark, table_name=descriptions_table_name, path=paths['descriptions_table_path'])

    #
    # Show results
    #
    utils.wait_for_stream(spark, query_name)
    spark.sql(f"SELECT * FROM delta.`{paths['silver_table_path']}`").limit(5).show()
    spark.sql(f"SELECT COUNT(*) FROM delta.`{paths['silver_table_path']}`").limit(5).show()


def main():
    """Main definition.
    :return: None
    """

    #
    # Get command line arguments
    #
    # get command line arguments
    args = get_args().parse_args()
    base_path = args.basePath
    database_name = args.databaseName

    #
    # Initialize the Spark application
    #
    spark, logger = utils.spark_init(app_name="WHO_Bronze_to_Silver")
    logger.info("Spark App is up and running.")

    #
    # Execute
    #
    who_bronze_to_silver(spark, base_path, database_name)

    logger.info("Spark App is terminating.")
    return None


def get_args():
    parser = argparse.ArgumentParser(description='Parameters')
    parser.add_argument('--sparkMaster', required=False, default="", help="Spark Master")
    parser.add_argument('--basePath', required=True, help="Base path of all data")
    parser.add_argument('--databaseName', required=True, help="Database for all tables")
    return parser


if __name__ == '__main__':
    main()
