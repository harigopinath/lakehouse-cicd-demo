# Databricks notebook source
# MAGIC %md
# MAGIC ##### Imports

# COMMAND ----------

from delta.tables import *

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.session import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ##### World Bank transformations

# COMMAND ----------

def process_wb_bronze(bronze_df: DataFrame) -> DataFrame:
    # Drop bad data
    wb_transformed_df = (bronze_df
                         .filter(~F.col("Indicator_Code").startswith("#"))
                         )

    # Keep only 2000-2018 at most
    wb_transformed_df = (wb_transformed_df
                         .withColumn("Year", F.col("Year").cast(T.IntegerType()))
                         .filter("Year >= 2000 AND Year <= 2018")
                         )

    # Can't use life expectancy from World Bank, or mortality rates or survival rates -- too closely related to life expectancy
    wb_transformed_df = (wb_transformed_df
                         .filter(~F.col("Indicator_Code").startswith("SP.DYN.LE"))
                         .filter(~F.col("Indicator_Code").startswith("SP.DYN.AMRT"))
                         .filter(~F.col("Indicator_Code").startswith("SP.DYN.TO"))
                         )

    # Don't use gender columns separately for now
    wb_transformed_df = (wb_transformed_df
                         .filter(~F.col("Indicator_Code").endswith(".FE") & ~F.col("Indicator_Code").endswith(".MA"))
                         .filter(~F.col("Indicator_Code").contains(".FE.") & ~F.col("Indicator_Code").contains(".MA."))
                         )

    # Don't use local currency variants
    wb_transformed_df = (wb_transformed_df
        .filter(~F.col("Indicator_Code").endswith(".CN") & ~F.col("Indicator_Code").endswith(".KN"))
        .filter(
        ~F.col("Indicator_Code").startswith("PA.") & ~F.col("Indicator_Code").startswith("PX."))
    )

    # Rename columns, while dropping everything but Year, Country, Indicator, and Value
    wb_transformed_df = (wb_transformed_df
        .select(
        F.col("Indicator_Code").alias("Indicator"),
        F.col("Indicator_Name").alias("Description"),
        F.col("Country_ISO3").alias("Country"),
        F.col("Year").cast("int"),
        F.col("Value").cast("float")
    )
    )

    # Replace NULLs with 0
    wb_transformed_df = wb_transformed_df.na.fill(0)

    return wb_transformed_df


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Overdoses transformations

# COMMAND ----------

def process_overdoses_bronze(bronze_df: DataFrame) -> DataFrame:
    from pyspark.sql.window import Window

    entity_schema = T.MapType(T.StringType(), T.StructType([
        T.StructField("name", T.StringType(), True),
        T.StructField("code", T.StringType(), True)
    ]))
    entities_df = (bronze_df
                   .withColumn("jsonData", F.from_json(F.col("entityKey"), entity_schema))
                   .select(F.explode("jsonData"))
                   .select(F.col("key").cast('long').alias("entityKey"), "value.*")
                   .drop_duplicates()
                   )

    variables_schema = T.MapType(T.StringType(), T.StructType([
        T.StructField("datasetName", T.StringType(), True),
        T.StructField("updatedAt", T.TimestampType(), True),
        T.StructField("years", T.ArrayType(T.StringType()), True),
        T.StructField("entities", T.ArrayType(T.LongType()), True),
        T.StructField("values", T.ArrayType(T.FloatType()), True)
    ]))

    w = Window.partitionBy("datasetName", "entities", "years").orderBy(F.col("updatedAt").desc())

    variables_df = (bronze_df
                    .withColumn("jsonData", F.from_json(F.col("variables"), variables_schema))
                    .select(F.explode("jsonData"))
                    .select("key", "value.*")
                    .select("datasetName", "updatedAt",
                            F.explode(F.arrays_zip("entities", "years", "values")).alias("data"))
                    .select("datasetName", "updatedAt", "data.*")
                    .withColumn("row", F.row_number().over(w))
                    .filter(F.col("row") == 1)
                    .drop("row")
                    .groupBy("entities", "years")
                    .pivot("datasetName")
                    .agg(F.first("values"))
                    .withColumnRenamed("entities", "entityKey")
                    )

    overdoses_df = (variables_df
                    .join(entities_df, on="entityKey", how="inner")
                    .drop("entityKey")
                    )

    # Keep only 2000-2018 at most
    overdoses_df = (overdoses_df
                    .withColumn("Year", F.col("years").cast(T.IntegerType()))
                    .filter("Year >= 2000 AND Year <= 2018")
                    )

    # Keep only countries, not territories
    overdoses_df = (overdoses_df
                    .filter(F.col("code").isNotNull())
                    )

    # Rename columns
    overdoses_df = (overdoses_df
                    .select("name", "code", "Year", "Cocaine use disorders", "Drug use disorders",
                            "Opioid use disorders", "Alcohol use disorders", "Other drug use disorders",
                            "Amphetamine use disorders")
                    .toDF("Country_Name", "Country_Code", "Year", "Cocaine_Deaths", "Illicit_Drug_Deaths",
                          "Opioids_Deaths", "Alcohol_Deaths", "Other_Illicit_Deaths", "Amphetamine_Deaths")
                    )

    # Replace NULLs with 0
    overdoses_df = overdoses_df.na.fill(0)

    return overdoses_df
