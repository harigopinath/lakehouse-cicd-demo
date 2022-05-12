# Databricks notebook source
# MAGIC %md
# MAGIC # Predicting Life Expectancy from Health Data
# MAGIC 
# MAGIC ![WHO](https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/ML/gartner_2020/WHO.png)
# MAGIC 
# MAGIC ## Data Preparation
# MAGIC 
# MAGIC The first major task is to access and evaluate the data. This work might be performed by a data engineering team, who is more familiar with Scala than, for example, Python. Within Databricks, both teams can choose the best language for their work and still collaborate easily even within a notebook.
# MAGIC 
# MAGIC Data comes from several sources.
# MAGIC 
# MAGIC - The WHO Health Indicators (primary data) for:
# MAGIC   - the USA: https://data.humdata.org/dataset/who-data-for-united-states-of-america
# MAGIC   - similarly for other developed nations: Australia, Denmark, Finland, France, Germany, Iceland, Italy, New Zealand, Norway, Portugal, Spain, Sweden, the UK
# MAGIC - The World Bank Health Indicators (supplementary data) for:
# MAGIC   - the USA: https://data.humdata.org/dataset/world-bank-combined-indicators-for-united-states
# MAGIC   - similarly for other developed nations
# MAGIC - Our World In Data (Drug Use)
# MAGIC   - https://ourworldindata.org/drug-use
# MAGIC   
# MAGIC ### Health Indicators primary data
# MAGIC 
# MAGIC The "health indicators" datasets are the primary data sets. They are CSV files, and are easily read by Spark. However, they don't have a consistent schema. Some contains extra "DATASOURCE" columns, which can be ignored.
# MAGIC 
# MAGIC ### Indicators supplementary data
# MAGIC The data from the World Bank can likewise be normalized, filtered and analyzed.
# MAGIC 
# MAGIC ### Overdoses
# MAGIC One issue that comes to mind when thinking about life expectancy, given the unusual downward trend in life expectancy in the USA, is drug-related deaths. These have been a newsworthy issue for the USA for several years. Our World In Data provides drug overdose data by country, year, and type.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![WHO](https://alexandrudatabrickseu.blob.core.windows.net/myfolder/arch.png?sp=r&st=2022-01-17T06:59:44Z&se=2022-01-31T14:59:44Z&spr=https&sv=2020-08-04&sr=b&sig=wV9bADxX4Qiilkd0N9KAUmtKsy5lX9QG8LL22USeibo%3D)