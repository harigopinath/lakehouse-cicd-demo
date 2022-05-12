# Databricks notebook source
# MAGIC %md
# MAGIC #### Jobs API v2.0 (classic)
# MAGIC - one task job: https://docs.databricks.com/dev-tools/api/latest/jobs.html#create
# MAGIC - because it's a one task job, it needs to use the ./run_all notebook which calls the other notebooks that form the pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create the job

# COMMAND ----------

import json
import requests
import sys
import time
import os

# Get the API url and bearer token
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
notebooks_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)

# Set the base URL and headers and Jobs API payload
base_url = f"{workspace_url.rstrip('/')}/api/2.0"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {access_token}"
}

# Cluster spec
cluster_spec = {
    "num_workers": 1,
    "node_type_id": "Standard_F4s_v2",
    "spark_version": "9.1.x-scala2.12"
}

# Job setting
job_name = "Life Expectancy Demo classic job"
job_schedule = {
    "quartz_cron_expression": "0 15 10 * * ? *",  # 10:15am every day
    "timezone_id": "UTC",
    "pause_status": "PAUSED"
}

# Notebook parameters
notebook_parameters_default = {
    "BasePath": "/tmp/alexandru-lakehouse",
    "DatabaseName": "alexandru_lakehouse_classic"
}

# Job API payload
payload = {
    "name": job_name,
    "new_cluster": cluster_spec,
    "schedule": job_schedule,
    "notebook_task": {
        "notebooks_path": f"{os.path.dirname(notebooks_path)}/run-all",
        "base_parameters": notebook_parameters_default
    },
    "format": "SINGLE_TASK"
}

# Create the job and get the job_id
response = requests.post(url=f"{base_url}/jobs/create", headers=headers, json=payload)
if response.status_code == requests.codes.ok:
    job_id = response.json()['job_id']
    print(f"job_id: {job_id}")
else:
    raise Exception(response.text)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Run the job

# COMMAND ----------

import json
import requests
import sys
import time
import os

# Get the API url and bearer token
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# Set the base URL and headers and Jobs API payload
base_url = f"{workspace_url.rstrip('/')}/api/2.0"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {access_token}"
}

# Notebook parameters
notebook_parameters_custom = {
    "BasePath": "/tmp/alexandru-lakehouse-test1",
    "DatabaseName": "alexandru_lakehouse_test1"
}

# Job API payload
payload = {
    "job_id": job_id,
    "notebook_params": notebook_parameters_custom
}

# Start the job and get the run id
response = requests.post(url=f"{base_url}/jobs/run-now", headers=headers, json=payload)
if response.status_code == requests.codes.ok:
    run_id = response.json()['run_id']
    print(f"run_id: {run_id}")
else:
    raise Exception(response.text)

# Wait for the job to finish
run_wait_time = 900  # 15 minutes
url = f"{workspace_url.rstrip('/')}/api/2.0/jobs/runs/get-output?run_id={run_id}"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {access_token}"
}

current_run_time = 0
response = requests.get(url=url, headers=headers)
print(f"Check the job run here: {response.json()['metadata']['run_page_url']}")
while current_run_time < run_wait_time:
    if response.status_code == requests.codes.ok:
        response_json = response.json()
        run_state = response_json["metadata"]["state"]["life_cycle_state"]
        if run_state == "INTERNAL_ERROR" or run_state == "SKIPPED":
            raise Exception(run_state)
        if run_state == "TERMINATED":
            result_state = response_json["metadata"]["state"]["result_state"]
            if result_state != "SUCCESS":
                raise Exception(result_state)
            if "notebook_output" in response_json.keys():
                print(response_json["notebook_output"])
            break
        current_run_time += 10
        print(f"Current state: {str(run_state)}. Sleeping for 10 seconds")
        print(f"Remaining: {str(run_wait_time - current_run_time)} seconds\n")
        time.sleep(10)

        response = requests.get(url=url, headers=headers)
    else:
        raise Exception(f"Error {str(response.status_code)}:\n{response.text}")
else:
    raise Exception(response.text)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Jobs API v2.1 (multitasks)
# MAGIC - multiple tasks job: https://docs.databricks.com/data-engineering/jobs/jobs-api-updates.html

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create the job

# COMMAND ----------

import json
import requests
import sys
import time
import os

# Get the API url and bearer token
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
notebooks_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)

# Set the base URL and headers and Jobs API payload
base_url = f"{workspace_url.rstrip('/')}/api/2.1"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {access_token}"
}

# Cluster spec
cluster_spec = {
    "num_workers": 0,
    "node_type_id": "Standard_F4s_v2",
    "spark_version": "9.1.x-scala2.12",
    "spark_conf": {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*, 4]"
    },
    "custom_tags": {
        "ResourceClass": "SingleNode"
    }
}

# Job setting
job_name = "Life Expectancy Demo multitask job"
job_schedule = {
    "quartz_cron_expression": "0 15 10 * * ? *",  # 10:15am every day
    "timezone_id": "UTC",
    "pause_status": "PAUSED"
}

# Notebook parameters
notebook_parameters_default = {
    "BasePath": "/tmp/alexandru-lakehouse",
    "DatabaseName": "alexandru_lakehouse_classic"
}

# Job API payload
payload = {
    "name": job_name,
    "schedule": job_schedule,
    "tasks": [
        {
            "new_cluster": cluster_spec,
            "notebook_task": {
                "notebooks_path": f"{os.path.dirname(notebooks_path)}/setup",
                "base_parameters": notebook_parameters_default
            },
            "task_key": "00-setup"
        },
        {
            "new_cluster": cluster_spec,
            "notebook_task": {
                "notebooks_path": f"{os.path.dirname(notebooks_path)}/01-world-bank-bronze",
                "base_parameters": notebook_parameters_default
            },
            "task_key": "01-world-bank-bronze",
            "depends_on": [
                {
                    "task_key": "00-setup"
                }
            ]
        },
        {
            "new_cluster": cluster_spec,
            "notebook_task": {
                "notebooks_path": f"{os.path.dirname(notebooks_path)}/02-world-bank-silver",
                "base_parameters": notebook_parameters_default
            },
            "task_key": "02-world-bank-silver",
            "depends_on": [
                {
                    "task_key": "01-world-bank-bronze"
                }
            ]
        },
        {
            "new_cluster": cluster_spec,
            "notebook_task": {
                "notebooks_path": f"{os.path.dirname(notebooks_path)}/01-who-bronze",
                "base_parameters": notebook_parameters_default
            },
            "task_key": "01-who-bronze",
            "depends_on": [
                {
                    "task_key": "00-setup"
                }
            ]
        },
        {
            "new_cluster": cluster_spec,
            "notebook_task": {
                "notebooks_path": f"{os.path.dirname(notebooks_path)}/02-who-silver",
                "base_parameters": notebook_parameters_default
            },
            "task_key": "02-who-silver",
            "depends_on": [
                {
                    "task_key": "01-who-bronze"
                }
            ]
        },
        {
            "new_cluster": cluster_spec,
            "notebook_task": {
                "notebooks_path": f"{os.path.dirname(notebooks_path)}/01-overdoses-bronze",
                "base_parameters": notebook_parameters_default
            },
            "task_key": "01-overdoses-bronze",
            "depends_on": [
                {
                    "task_key": "00-setup"
                }
            ]
        },
        {
            "new_cluster": cluster_spec,
            "notebook_task": {
                "notebooks_path": f"{os.path.dirname(notebooks_path)}/02-overdoses-silver",
                "base_parameters": notebook_parameters_default
            },
            "task_key": "02-overdoses-silver",
            "depends_on": [
                {
                    "task_key": "01-overdoses-bronze"
                }
            ]
        },
        {
            "new_cluster": cluster_spec,
            "notebook_task": {
                "notebooks_path": f"{os.path.dirname(notebooks_path)}/03-indicators-gold",
                "base_parameters": notebook_parameters_default
            },
            "task_key": "03-indicators-gold",
            "depends_on": [
                {
                    "task_key": "02-who-silver"
                },
                {
                    "task_key": "02-world-bank-silver"
                },
                {
                    "task_key": "02-overdoses-silver"
                }
            ]
        }
    ],
    "format": "MULTI_TASK"
}

# Create the job and get the job_id
response = requests.post(url=f"{base_url}/jobs/create", headers=headers, json=payload)
if response.status_code == requests.codes.ok:
    job_id = response.json()['job_id']
    print(f"job_id: {job_id}")
else:
    raise Exception(response.text)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Run the job

# COMMAND ----------

import json
import requests
import sys
import time
import os

# Get the API url and bearer token
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# Set the base URL and headers and Jobs API payload
base_url = f"{workspace_url.rstrip('/')}/api/2.0"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {access_token}"
}

# Notebook parameters
notebook_parameters_custom = {
    "BasePath": "/tmp/alexandru-lakehouse-test2",
    "DatabaseName": "alexandru_lakehouse_test2"
}

# Job API payload
payload = {
    "job_id": job_id,
    "notebook_params": notebook_parameters_custom
}

# Start the job and get the run id
response = requests.post(url=f"{base_url}/jobs/run-now", headers=headers, json=payload)
if response.status_code == requests.codes.ok:
    run_id = response.json()['run_id']
    print(f"run_id: {run_id}")
else:
    raise Exception(response.text)

# Wait for the job to finish
run_wait_time = 1800  # 30 minutes
url = f"{workspace_url.rstrip('/')}/api/2.0/jobs/runs/get?run_id={run_id}"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {access_token}"
}

current_run_time = 0
response = requests.get(url=url, headers=headers)
print(f"Check the job run here: {response.json()['run_page_url']}")
while current_run_time < run_wait_time:
    if response.status_code == requests.codes.ok:
        response_json = response.json()
        run_state = response_json["state"]["life_cycle_state"]
        if run_state == "INTERNAL_ERROR" or run_state == "SKIPPED":
            raise Exception(run_state)
        if run_state == "TERMINATED":
            result_state = response_json["state"]["result_state"]
            if result_state != "SUCCESS":
                raise Exception(result_state)
            break
        current_run_time += 10
        print(f"Current state: {str(run_state)}. Sleeping for 10 seconds")
        print(f"Remaining: {str(run_wait_time - current_run_time)} seconds\n")
        time.sleep(10)

        response = requests.get(url=url, headers=headers)
    else:
        raise Exception(f"Error {str(response.status_code)}:\n{response.text}")
else:
    raise Exception(response.text)
