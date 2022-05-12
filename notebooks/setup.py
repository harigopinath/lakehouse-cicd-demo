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
# MAGIC ### Create base resources

# COMMAND ----------

dbutils.fs.mkdirs(base_path)

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

import os
os.environ["WHO_LOCATION"] = who_landing_path
os.environ["WB_LOCATION"] = wb_landing_path
os.environ["OVERDOSES_LOCATION"] = overdoses_landing_path

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare the WHO data

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC who_source_files=$(cat <<END
# MAGIC health_indicators_usa.csv|https://data.humdata.org/dataset/0403b8f6-6fb6-475b-b910-e5b55adb1f8b/resource/1ae227c9-01b6-4698-804e-3109d2c1ba2b/download/health_indicators_usa.csv
# MAGIC health_indicators_gbr.csv|https://data.humdata.org/dataset/1c00c076-0d17-46cf-8dce-8e901d09a3e2/resource/8222782d-4631-4673-9bab-79c834400fef/download/health_indicators_gbr.csv
# MAGIC health_indicators_aus.csv|https://data.humdata.org/dataset/b58ad2a5-fcd0-4208-8d26-5a156728cb5d/resource/78f12167-a36e-470b-925b-df37781a1623/download/health_indicators_aus.csv
# MAGIC #health_indicators_aus_dup.csv|https://data.humdata.org/dataset/b58ad2a5-fcd0-4208-8d26-5a156728cb5d/resource/78f12167-a36e-470b-925b-df37781a1623/download/health_indicators_aus.csv
# MAGIC health_indicators_nzl.csv|https://data.humdata.org/dataset/ac38fb79-a999-4a8b-ae79-24686db94e33/resource/e8305160-3410-4e32-9de0-3b45945d6daa/download/health_indicators_nzl.csv
# MAGIC health_indicators_dnk.csv|https://data.humdata.org/dataset/fd0e4765-b4b7-4cd2-bdd1-6868726e95fd/resource/8a9a7f78-da94-4229-a7f7-b25267affaf0/download/health_indicators_dnk.csv
# MAGIC health_indicators_fin.csv|https://data.humdata.org/dataset/12e18fc8-64ab-48fd-ae3b-ca2ef8c14c93/resource/d5edc4ec-3628-4f00-ae0d-467f26cef79e/download/health_indicators_fin.csv
# MAGIC health_indicators_fra.csv|https://data.humdata.org/dataset/81123758-cbee-40bd-8375-a3175863a5ae/resource/d446e934-d083-4fcd-8ca2-4e221870a1b5/download/health_indicators_fra.csv
# MAGIC health_indicators_deu.csv|https://data.humdata.org/dataset/261361f2-a9e5-4005-8479-2be53af912de/resource/250a326b-e218-408b-9ab2-f86073308a19/download/health_indicators_deu.csv
# MAGIC health_indicators_isl.csv|https://data.humdata.org/dataset/25c1e8bd-6bea-4349-aa33-64dc99d80364/resource/667cf797-814e-4b46-8355-c86dd65bb1d0/download/health_indicators_isl.csv
# MAGIC health_indicators_ita.csv|https://data.humdata.org/dataset/ee46627a-a76c-438f-927b-7d66e288badc/resource/b77bb4a8-a183-43eb-bf26-05480a4dc9a9/download/health_indicators_ita.csv
# MAGIC health_indicators_nor.csv|https://data.humdata.org/dataset/31688a7c-7aae-464b-b876-eea1c20869fc/resource/13ff36a5-1ad1-4c03-b8aa-f24e463cac71/download/health_indicators_nor.csv
# MAGIC health_indicators_prt.csv|https://data.humdata.org/dataset/7293e47c-1528-453e-953d-c5ff5d43619b/resource/b89af122-d4e8-4f93-8001-880a65aa1887/download/health_indicators_prt.csv
# MAGIC health_indicators_esp.csv|https://data.humdata.org/dataset/9e3b4742-5388-4184-9d26-4ed10eea6fa5/resource/eac800fa-b84f-4063-9a3a-e26aea1fbfa7/download/health_indicators_esp.csv
# MAGIC #health_indicators_swe.csv|https://data.humdata.org/dataset/0d061106-7e12-4ffd-922a-3cfd4c9a1e3e/resource/7218f044-af36-40ad-83fe-cfb3a9fbec92/download/health_indicators_swe.csv
# MAGIC END
# MAGIC )
# MAGIC 
# MAGIC mkdir -p ${WHO_LOCATION} || exit 1
# MAGIC cd ${WHO_LOCATION} || exit 1
# MAGIC 
# MAGIC IFS=$'\n'
# MAGIC for line in $(echo "${who_source_files}"); do
# MAGIC   file_name=$(echo ${line} | cut -d'|' -f1)
# MAGIC   file_url=$(echo ${line} | cut -d'|' -f2)
# MAGIC   [[ -f ${file_name} ]] || wget -q ${file_url} -O ${file_name}
# MAGIC done
# MAGIC 
# MAGIC mkdir -p "/dbfs${WHO_LOCATION}"
# MAGIC cp -a ${WHO_LOCATION}/* /dbfs${WHO_LOCATION}

# COMMAND ----------

dbutils.fs.ls(who_landing_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare the World Bank data

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC world_bank_source_files=$(cat <<END
# MAGIC indicators_usa.csv|https://data.humdata.org/dataset/579c7dac-b607-4f3b-8cd4-c97b7c219843/resource/02d50c9b-2687-4805-8e79-aa179ff75ccf/download/indicators_usa.csv
# MAGIC indicators_gbr.csv|https://data.humdata.org/dataset/3872d3b8-efb8-4f5e-9091-4b638852d81b/resource/37ca3ab4-1818-473a-b175-096c149d8386/download/indicators_gbr.csv
# MAGIC indicators_aus.csv|https://data.humdata.org/dataset/26cd17d4-aa56-4554-848e-e7b840d1a410/resource/66aefd03-81d4-4596-bb71-8864c5dbdd39/download/indicators_aus.csv
# MAGIC indicators_nzl.csv|https://data.humdata.org/dataset/1dae6f29-c7be-434b-be14-b00b460ec238/resource/deca8708-8e9f-4189-9dfa-c87752def0c7/download/indicators_nzl.csv
# MAGIC indicators_dnk.csv|https://data.humdata.org/dataset/7ed83bad-3341-499f-bd15-ea7a072a7636/resource/41bf577f-f656-447b-9c40-8ec60c765395/download/indicators_dnk.csv
# MAGIC indicators_fin.csv|https://data.humdata.org/dataset/58fc429a-1bd9-4c58-9716-9b4846b59177/resource/f63f2ccb-0b92-426f-a461-74690acb951a/download/indicators_fin.csv
# MAGIC indicators_fra.csv|https://data.humdata.org/dataset/c809d1d4-974d-4bb5-b714-23e2bde97bb8/resource/e82b729e-b264-41d2-9956-f7e46ffe99b8/download/indicators_fra.csv
# MAGIC indicators_deu.csv|https://data.humdata.org/dataset/79757b0c-9017-4993-a2b2-99e75bf95de2/resource/a6a7bce0-4fd2-46e0-b22f-e68d65efd2fe/download/indicators_deu.csv
# MAGIC indicators_isl.csv|https://data.humdata.org/dataset/c23fadc1-4a80-4ce9-80e4-c7fb05889bed/resource/c1710e00-a808-4342-9652-0d889cd7c67f/download/indicators_isl.csv
# MAGIC indicators_ita.csv|https://data.humdata.org/dataset/53e4a3e5-f54d-4797-a2a4-59095891d4c7/resource/f5a527ee-8b65-44e9-8562-a5aa4bc0b0d0/download/indicators_ita.csv
# MAGIC indicators_nor.csv|https://data.humdata.org/dataset/d9017faf-3748-4c8b-8be5-660c782e3ed3/resource/883b319f-2660-4774-b535-348a17070f75/download/indicators_nor.csv
# MAGIC indicators_prt.csv|https://data.humdata.org/dataset/f05ec5eb-fcac-4742-8e6d-7b87c225ef57/resource/66a65553-4119-449a-986e-83cdd75920df/download/indicators_prt.csv
# MAGIC indicators_esp.csv|https://data.humdata.org/dataset/f8f510e5-936a-41d3-a640-1f4b52b47894/resource/20af94cf-d9d1-4305-ba6d-857e065b5b02/download/indicators_esp.csv
# MAGIC #indicators_swe.csv|https://data.humdata.org/dataset/03628271-1aa9-4d80-9fee-bbdac06d12b4/resource/32f9db65-b488-4b3c-a42d-be0f3c375725/download/indicators_swe.csv
# MAGIC END
# MAGIC )
# MAGIC 
# MAGIC mkdir -p ${WB_LOCATION} || exit 1
# MAGIC cd ${WB_LOCATION} || exit 1
# MAGIC 
# MAGIC IFS=$'\n'
# MAGIC for line in $(echo "${world_bank_source_files}"); do
# MAGIC   file_name=$(echo ${line} | cut -d'|' -f1)
# MAGIC   file_url=$(echo ${line} | cut -d'|' -f2)
# MAGIC   [[ -f ${file_name} ]] || wget -q ${file_url} -O ${file_name}
# MAGIC done
# MAGIC 
# MAGIC mkdir -p "/dbfs${WB_LOCATION}"
# MAGIC cp -a ${WB_LOCATION}/* /dbfs${WB_LOCATION}

# COMMAND ----------

dbutils.fs.ls(wb_landing_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare the Overdoses data

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC mkdir -p ${OVERDOSES_LOCATION} || exit 1
# MAGIC cd ${OVERDOSES_LOCATION} || exit 1
# MAGIC 
# MAGIC file_name=overdoses.json
# MAGIC [[ -f ${file_name} ]] || wget -q https://ourworldindata.org/grapher/data/variables/58639+58613+58626+58602+61916+58652.json?v=1 -O ${file_name}
# MAGIC 
# MAGIC mkdir -p "/dbfs${OVERDOSES_LOCATION}"
# MAGIC cp -a ${OVERDOSES_LOCATION}/* /dbfs${OVERDOSES_LOCATION}

# COMMAND ----------

dbutils.fs.ls(overdoses_landing_path)