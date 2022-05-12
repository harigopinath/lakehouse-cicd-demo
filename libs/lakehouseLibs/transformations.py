from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from delta.tables import *


def process_who_bronze(bronze_df: DataFrame) -> DataFrame:
    # Drop bad data
    who_transformed_df = (bronze_df
                          .filter(~F.col("GHO_CODE").startswith("#"))
                          )

    # Can't use life expectancy at 60
    who_transformed_df = who_transformed_df.filter(F.col("GHO_CODE") != "WHOSIS_000015")

    # Keep just PUBLISHED data, not VOID
    who_transformed_df = who_transformed_df.filter(F.col("PUBLISHSTATE_CODE") == "PUBLISHED").drop("PUBLISHSTATE_CODE")

    # Use stats for both sexes now, not male/female separately. It's either NULL or BTSX
    who_transformed_df = who_transformed_df.filter((F.col("SEX_CODE").isNull()) | (F.col("SEX_CODE") == "BTSX")).drop(
        "SEX_CODE")

    # Use Numeric where available, otherwise Display Value, as value
    who_transformed_df = (who_transformed_df
                          .withColumn("Value", F.when(F.col("Numeric").isNull(), F.col("Numeric")).otherwise(
        F.col("Numeric")).cast(T.FloatType()))
                          )

    # Some "year" values are like 2012-2017. Explode to a value for each year in the range
    def years_to_range(s):
        if "-" not in str(s):
            return [s]
        else:
            return s.split("-")

    years_to_range_udf = F.udf(years_to_range, T.ArrayType(T.StringType()))

    who_transformed_df = (who_transformed_df
                          .withColumn("Year", F.explode(years_to_range_udf(F.col("YEAR_CODE"))))
                          .withColumn("Year", F.col("Year").cast(T.IntegerType()))
                          )

    # Rename columns, while dropping everything but Year, Country, GHO CODE, and Value
    who_transformed_df = (who_transformed_df
                          .select(F.col("GHO_CODE").alias("Indicator"),
                                  F.col("GHO_DISPLAY").alias("Description"),
                                  F.col("COUNTRY_CODE").alias("Country"),
                                  F.col("Year").cast("int"),
                                  F.col("Value").cast("float")
                                  )
                          )

    # Keep only 2000-2018 at most
    who_transformed_df = who_transformed_df.filter("Year >= 2000 AND Year <= 2018")

    # Replace NULLs with 0
    who_transformed_df = who_transformed_df.na.fill(0)

    return who_transformed_df
