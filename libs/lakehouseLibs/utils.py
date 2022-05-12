import os
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from delta.tables import *
from delta.exceptions import ProtocolChangedException


def fix_columns(col: str) -> str:
    replacements = {" ": "_", "(": "", ")": ""}
    #replacements = {" ": "", "(": "_", ")": ""}
    return "".join([replacements.get(c, c) for c in col])


def fix_dataframe_columns(df: DataFrame) -> DataFrame:
    fixed_columns = map(fix_columns, df.columns)
    return df.toDF(*fixed_columns)


def wait_for_stream(spark: SparkSession, name: str):
    import time
    queries = list(filter(lambda query: query.name == name, spark.streams.active))

    while len(queries) > 0 and len(queries[0].recentProgress) < 2:
        time.sleep(5)
        queries = list(filter(lambda query: query.name == name, spark.streams.active))


def spark_init(master: str = None, app_name: str = "Spark_App") -> tuple:
    """Start Spark session and get Spark logger.
    """
    #
    # Start the Spark application and get the Spark session
    #
    if not master:
        master = "local"
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            master = ""
    if len(master) > 0:
        from delta import configure_spark_with_delta_pip
        builder = (SparkSession
                   .builder
                   .master(master)
                   .appName(app_name)
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                   )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        spark = (SparkSession
                 .builder
                 .appName(app_name)
                 .getOrCreate()
                 )

    #
    # Extract the spark app name from the config
    #
    conf = spark.sparkContext.getConf()
    app_name = conf.get('spark.app.name')

    #
    # Get the log4j object
    #
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger(app_name)

    return spark, logger


def read_stream_csv(spark: SparkSession, path: str, schema: T.StructType() = None,
                    max_files_per_trigger: int = 10) -> DataFrame:
    if not schema:
        df = (spark
              .read
              .format("csv")
              .option("inferSchema", True)
              .option("header", True)
              .load(path)
              )
        schema = df.schema

    return (spark
            .readStream
            #.format("cloudFiles")
            #.option("cloudFiles.format", "csv")
            .format("csv")
            .option("header", True)
            .schema(schema)
            .option("maxFilesPerTrigger", max_files_per_trigger)
            .load(path)
            )


def read_stream_json(spark: SparkSession, path: str, schema: T.StructType() = None,
                     max_files_per_trigger: int = 10) -> DataFrame:
    if not schema:
        schema = spark.read.format("json").load(path).schema

    return (spark
            .readStream
            .format("json")
            .option("maxFilesPerTrigger", max_files_per_trigger)
            .schema(schema)
            .load(path)
            )


def read_stream_delta(spark: SparkSession, path: str, max_files_per_trigger: int = 10,
                      max_bytes_per_trigger: int = 10 * 1024 * 1024) -> DataFrame:
    return (spark
            .readStream
            .format("delta")
            .option("maxFilesPerTrigger", max_files_per_trigger)
            .option("maxBytesPerTrigger", max_bytes_per_trigger)
            .load(path)
            )


def init_delta_table(spark: SparkSession, schema: T.StructType, path: str, partition_columns: list = None) -> None:
    writer = (spark
              .createDataFrame(spark.sparkContext.parallelize([]), schema)
              .limit(0)
              .write
              .format("delta")
              .mode("ignore")
              )

    if partition_columns is not None:
        writer = writer.partitionBy(*partition_columns)

    try:
        writer.save(path)
    except ProtocolChangedException as e:
        import time
        time.sleep(30)  # sleep 30s
        writer.save(path)  # retry

def register_delta_table(spark: SparkSession, table_name: str, path: str, drop_if_exists: bool = False) -> None:
    if drop_if_exists:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} "
              "USING DELTA "
              f"LOCATION '{path}'")
