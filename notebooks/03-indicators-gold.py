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

# MAGIC %run ./includes/common

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checks

# COMMAND ----------

print(health_indicators_silver_table_path)
print(overdoses_silver_table_path)
print(health_indicators_gold_table_path)
print(all_indicators_gold_table_path)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{health_indicators_silver_table_path}`").limit(5))

# COMMAND ----------

display(spark.sql("SELECT * FROM delta.`{}`".format(overdoses_silver_table_path)).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver to Gold pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the Silver data

# COMMAND ----------

indicators_df = (spark
                 .read
                 .format("delta")
                 .load(health_indicators_silver_table_path)
                 )

# COMMAND ----------

overdoses_df = (spark
                .read
                .format("delta")
                .load(overdoses_silver_table_path)
                )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW descriptions
# MAGIC AS
# MAGIC SELECT * FROM silver_health_indicators_descriptions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the most useful features

# COMMAND ----------

# indicators_pivot_df = (indicators_df
#   .groupBy("Country", "Year")
#   .pivot("Indicator")
#   .avg("Value")
#   .orderBy("Year")
#   .na.fill(0)
# )

# indicators_pivot_df.persist()
# indicators_pivot_df.createOrReplaceTempView("healthIndicatorsPivotView")

# COMMAND ----------

# %sql
# SELECT * FROM healthIndicatorsPivotView
# WHERE Year < 2003
# ORDER BY Year

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a pivot for selected indicators

# COMMAND ----------

selected_indicators = [
 "WHOSIS_000001",       # Target - life expectancy
 "WHOSIS_000004",       # Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)
 "WHS7_156",            # Per capita total expenditure on health at average exchange rate (US$)
 "WHS9_93",             # Gross national income per capita (PPP int. $)
 "NY.GDP.PCAP.CD",      # GDP per capita (current US$)
 "BX.TRF.PWKR.CD.DT",   # Personal remittances, received (current US$)
 "EN.ATM.CO2E.LF.ZS",   # CO2 emissions from liquid fuel consumption (% of total)
 "GFDD.OI.02",          # Bank deposits to GDP (%)
 "HWF_0001",            # Medical doctors (per 10 000 population)
 "SE.TER.GRAD.HL.ZS",   # Percentage of graduates from tertiary education graduating from Health and Welfare programmes, both sexes (%)
]

#selected_indicators_df = indicators_pivot_df.select(["Country", "Year"] + list(map(lambda i: f"`{i}`", selected_indicators)))

selected_indicators_df = (indicators_df
                          .filter(F.col("Indicator").isin(selected_indicators))
                          .groupBy("Country", "Year")
                          .pivot("Indicator")
                          .avg("Value")
                          .orderBy("Year")
                          .na.fill(0)
                          )

selected_indicators_df.createOrReplaceTempView("healthIndicatorsView")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM descriptions
# MAGIC WHERE Indicator IN ("WHOSIS_000004", "WHS7_156", "WHS9_93", "NY.GDP.PCAP.CD", "BX.TRF.PWKR.CD.DT", "EN.ATM.CO2E.LF.ZS", "GFDD.OI.02", "HWF_0001", "SE.TER.GRAD.HL.ZS")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM healthIndicatorsView LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a pivot for overdoses data

# COMMAND ----------

overdoses_df.createOrReplaceTempView("overdosesView")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM overdosesView LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join the selected indicators with the overdoses data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW allIndicatorsView
# MAGIC AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT
# MAGIC     o.Country_Name AS Country,
# MAGIC     o.Year AS Year,
# MAGIC     WHOSIS_000001 AS Life_Expectancy,
# MAGIC     WHOSIS_000004 AS Adult_Mortality_Rate,
# MAGIC     WHS7_156 AS Health_Expenditure,
# MAGIC     `NY.GDP.PCAP.CD` AS GDP_Capita,
# MAGIC     `BX.TRF.PWKR.CD.DT` AS Remittances,
# MAGIC     `EN.ATM.CO2E.LF.ZS` AS Emissions_Fuel,
# MAGIC     `GFDD.OI.02` AS Bank_Deposit ,
# MAGIC     HWF_0001 AS Medical_Doctors,
# MAGIC     `SE.TER.GRAD.HL.ZS` AS University_Graduates,
# MAGIC     Cocaine_Deaths,
# MAGIC     Illicit_Drug_Deaths,
# MAGIC     Opioids_Deaths,
# MAGIC     Alcohol_Deaths,
# MAGIC     Other_Illicit_Deaths,
# MAGIC     Amphetamine_Deaths
# MAGIC   FROM healthIndicatorsView h
# MAGIC   LEFT OUTER JOIN overdosesView o
# MAGIC     ON h.Year = o.Year AND h.Country = o.Country_Code
# MAGIC )
# MAGIC WHERE Year IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM allIndicatorsView
# MAGIC ORDER BY Country, Year

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the health indicators Gold table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE """ + indicators_gold_table_name + """
USING DELTA
LOCATION '""" + health_indicators_gold_table_path + """'
AS
SELECT
  *
FROM healthIndicatorsView
""")

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the all indicators Gold table

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare the Dataframe and table

# COMMAND ----------

# Store the view in a Dataframe
all_indicators_df = spark.sql("SELECT * FROM allIndicatorsView")

# Create the Delta Gold table if it doesn't exist
init_delta_table(spark, all_indicators_df.schema, all_indicators_gold_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge with the existing table

# COMMAND ----------

# Get the Delta table object
delta_table = DeltaTable.forPath(spark, all_indicators_gold_table_path)

# Perform the MERGE using Python syntax
(delta_table.alias("dest").merge(
    all_indicators_df.alias("src"), "src.Year = dest.Year and src.Country = dest.Country")
 .whenNotMatchedInsertAll()
 .whenMatchedUpdateAll()
 .execute()
 )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Register the table in the Metastore

# COMMAND ----------

register_delta_table(spark, all_gold_table_name, all_indicators_gold_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check results

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{health_indicators_gold_table_path}` ORDER BY Country, Year LIMIT 10"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{all_indicators_gold_table_path}` ORDER BY Country, Year LIMIT 10"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM gold_all_indicators
# MAGIC WHERE Year <= 2016
# MAGIC ORDER BY Year ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Country,
# MAGIC   Medical_Doctors
# MAGIC FROM gold_all_indicators
# MAGIC WHERE Year <= 2016
# MAGIC ORDER BY Year ASC