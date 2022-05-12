import pytest
import os

from pyspark.sql import types as T
from pyspark.sql import functions as F
from delta.tables import *

from lakehouseLibs import utils, transformations


@pytest.fixture(scope="session")
def spark_init():
    spark, logger = utils.spark_init(app_name="Spark_Test_App")
    yield spark, logger
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        spark.stop()


def test_process_who_bronze(spark_init):
    """Test process_who_bronze"""

    spark, logger = spark_init

    #
    # Set test data
    #
    test_data = [
        ('#indicator+code', '#indicator+name', '#indicator+url', None, None, None, '#status+code', '#status+name', None, None, '#date+year', None, '#region+code', '#date+year+start', '#date+year+end', '#region+name', None, None, None, None, '#country+code', '#country+name', None, None, None, None, '#sex+code', '#sex+name', None, None, None, None, None, None, None, None, '#indicator+value+num', None, None, None, None, None, "2022-02-14"),
        ('WHOSIS_000015', 'Life expectancy at age 60 (years)', 'https://www.who.int/data/gho/indicator-metadata-registry/imr-details/2977', None, None, None, 'PUBLISHED', 'Published', None, '2015', '2015', None, 'EUR', '2015', '2015', 'Europe', None, None, None, None, 'FRA', 'France', None, None, None, None, 'MLE', 'Male', None, None, None, None, None, None, None, '22.9', '22.85283', None, None, None, None, None, "2022-02-14"),
        ('MORT_100', 'Number of deaths', 'https://www.who.int/data/gho/indicator-metadata-registry/imr-details/3365', None, None, None, 'PUBLISHED', 'Published', None, '2000', '2000', None, 'EUR', '2000', '2000', 'Europe', None, None, None, None, 'FRA', 'France', None, 'DAYS0-27', '0-27 days', None, None, None, None, None, None, None, 'CH17', 'InjuriesInjuries', None, '19', '18.53127', None, None, None, None, None, "2022-02-14"),
        ('WHOSIS_000004', 'Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)', 'https://www.who.int/data/gho/indicator-metadata-registry/imr-details/64', 'None', 'None', 'None', 'PUBLISHED', 'Published', 'None', '2003-2005', '2000-2001', 'None', 'EUR', '2000', '2000', 'Europe', 'None', 'None', 'None', 'None', 'FRA', 'France', 'None', 'None', 'None', 'None', 'BTSX', 'Female', 'None', 'None', 'None', 'None', 'None', 'None', 'None', '61', '61.12841', 'None', 'None', 'None', 'None', 'None', "2022-02-14")
    ]

    test_df = spark.createDataFrame(test_data, ['GHO_CODE', 'GHO_DISPLAY', 'GHO_URL', 'GBDCHILDCAUSES_CODE', 'GBDCHILDCAUSES_DISPLAY', 'GBDCHILDCAUSES_URL', 'PUBLISHSTATE_CODE', 'PUBLISHSTATE_DISPLAY', 'PUBLISHSTATE_URL', 'YEAR_CODE', 'YEAR_DISPLAY', 'YEAR_URL', 'REGION_CODE', 'STARTYEAR', 'ENDYEAR', 'REGION_DISPLAY', 'REGION_URL', 'WORLDBANKINCOMEGROUP_CODE', 'WORLDBANKINCOMEGROUP_DISPLAY', 'WORLDBANKINCOMEGROUP_URL', 'COUNTRY_CODE', 'COUNTRY_DISPLAY', 'COUNTRY_URL', 'AGEGROUP_CODE', 'AGEGROUP_DISPLAY', 'AGEGROUP_URL', 'SEX_CODE', 'SEX_DISPLAY', 'SEX_URL', 'GHECAUSES_CODE', 'GHECAUSES_DISPLAY', 'GHECAUSES_URL', 'CHILDCAUSE_CODE', 'CHILDCAUSE_DISPLAY', 'CHILDCAUSE_URL', 'Display_Value', 'Numeric', 'Low', 'High', 'StdErr', 'StdDev', 'Comments', 'ingestion_date'])

    expected_data = [
        ("MORT_100", "Number of deaths", "FRA", "2000", "18.53127"),
        ("WHOSIS_000004", "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)", "FRA", "2003", "61.12841"),
        ("WHOSIS_000004", "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)", "FRA", "2005", "61.12841")
    ]

    expected_df = (spark.createDataFrame(expected_data, ["Indicator", "Description", "Country", "Year", "Value"])
                   .withColumn("Year", F.col("Year").cast("int"))
                   .withColumn("Value", F.col("Value").cast("float"))
                   )

    #
    # Run the assertions
    #
    processed_df = transformations.process_who_bronze(test_df)
    assert processed_df.toPandas().equals(expected_df.toPandas())
