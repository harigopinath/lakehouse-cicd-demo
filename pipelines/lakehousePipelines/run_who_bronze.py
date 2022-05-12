import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouseLibs import utils


def set_paths(base_path: str) -> dict:
    landing_base_path = f"{base_path.rstrip('/')}/health-data-source"
    pipeline_base_path = f"{base_path.rstrip('/')}/classic/life-expectancy"

    bronze_tables_base_path = f"{pipeline_base_path}/bronze"
    landing_path = f"{landing_base_path}/who"
    bronze_table_path = f"{bronze_tables_base_path}/who_indicators.delta"
    bronze_checkpoint_path = f"{bronze_tables_base_path}/who_indicators.checkpoint"

    return {
        "landing_path": landing_path,
        "bronze_table_path": bronze_table_path,
        "bronze_checkpoint_path": bronze_checkpoint_path
    }


def who_landing_to_bronze(spark: SparkSession, base_path: str) -> None:
    paths = set_paths(base_path)

    #
    # Checks
    #
    print(paths)
    spark.sql(f"SELECT * FROM csv.`{paths['landing_path']}` LIMIT 5").show()

    #
    # Landing to Bronze stream
    #
    # read the data stream and add the partition column
    stream_df = utils.fix_dataframe_columns(utils.read_stream_csv(spark, paths["landing_path"])
                                            .withColumn("ingestion_date", F.current_timestamp().cast("date"))
                                            )
    query_name = "LandingToBronze"

    (stream_df
     .writeStream
     .format("delta")
     .outputMode("append")
     .option("checkpointLocation", paths["bronze_checkpoint_path"])
     .queryName(query_name)
     .partitionBy("ingestion_date")
     .trigger(once=True)
     #  .trigger(processingTime="2 minutes")                            # Configure for a 2-minutes micro-batch
     .start(paths["bronze_table_path"])
     )

    #
    # Show results
    #
    utils.wait_for_stream(spark, query_name)
    spark.sql(f"SELECT * FROM delta.`{paths['bronze_table_path']}`").limit(5).show()
    spark.sql(f"SELECT COUNT(*) FROM delta.`{paths['bronze_table_path']}`").limit(5).show()


def get_args():
    parser = argparse.ArgumentParser(description='Parameters')
    parser.add_argument('--sparkMaster', required=False, default="", help="Spark Master")
    parser.add_argument('--basePath', required=True, help="Base path of all data")
    return parser


def main() -> None:
    """Main definition.
    :return: None
    """

    #
    # Get command line arguments
    #
    # get command line arguments
    args = get_args().parse_args()
    base_path = args.basePath

    #
    # Initialize the Spark application
    #
    spark, logger = utils.spark_init(app_name="WHO_Landing_to_Bronze")
    logger.info("Spark App is up and running.")

    #
    # Execute
    #
    who_landing_to_bronze(spark, base_path)

    logger.info("Spark App is terminating.")
    return None


if __name__ == '__main__':
    main()
