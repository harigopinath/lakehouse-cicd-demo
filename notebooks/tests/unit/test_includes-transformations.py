# Databricks notebook source
# MAGIC %md
# MAGIC ### Import the functions

# COMMAND ----------

# MAGIC %run ../../includes/transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare input DataFrame

# COMMAND ----------

test_data = [
    ("#country+name","#country+code","#date+year","#indicator+name","#indicator+code","#indicator+value+num","2022-02-14"),
    ("France","FRA","2005","Agricultural machinery, tractors","AG.AGR.TRAC.NO","1176425","2022-02-14"),
    ("United Kingdom","GBR","1983","Fertilizer consumption (% of fertilizer production)","AG.CON.FERT.PT.ZS","129.998509761065","2022-02-14"),
    ("Italy","ITA","2017","Surface area (sq. km)","AG.SRF.TOTL.K2","301340","2022-02-14")
]

test_df = spark.createDataFrame(test_data, ["Country_Name", "Country_ISO3", "Year", "Indicator_Name", "Indicator_Code", "Value", "ingestion_date"])

# COMMAND ----------

expected_data = [
    ("AG.AGR.TRAC.NO", "Agricultural machinery, tractors", "FRA", "2005", "1176425"),
    ("AG.SRF.TOTL.K2", "Surface area (sq. km)", "ITA", "2017", "301340")
]

expected_df = spark.createDataFrame(expected_data, ["Indicator", "Description", "Country", "Year", "Value"])

expected_df = (expected_df
               .withColumn("Year", F.col("Year").cast("int"))
               .withColumn("Value", F.col("Value").cast("float"))
              )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test process_wb_bronze()

# COMMAND ----------

processed_df = process_wb_bronze(test_df)

# COMMAND ----------

assert processed_df.toPandas().equals(expected_df.toPandas())