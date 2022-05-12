#!/usr/bin/env bash
#
# Copy files to DBFS
#

# Required parameters
_data_path=${1}
_notebooks_path=${2}
_libs_path=${3}
_libs_version=${4}
_database_name=${5}
_job_name=${6}
_cli_profile=${7}

_usage() {
  echo -e "Usage: ${0} <_data_path> <_notebooks_path> <_libs_path> <_libs_version> <_database_name> <_job_name>"
  exit 1
}

# Parameters check
[ -z "${_data_path}" ] && _usage
[ -z "${_notebooks_path}" ] && _usage
[ -z "${_libs_path}" ] && _usage
[ -z "${_libs_version}" ] && _usage
[ -z "${_database_name}" ] && _usage
[ -z "${_job_name}" ] && _usage
if [ -z "${_cli_profile}" ]; then
  _cli_profile=DEFAULT
fi

# Check if there is a job already defined
job_id=$(databricks jobs list | grep "${_job_name}" | tail -1 | cut -d' ' -f1)
if [ -z "$job_id" ];
then
  task="create"
else
  task="reset --job-id ${job_id}"
fi

# Switch to Jobs API 2.1
databricks jobs configure --version=2.1
sleep 1

# Create the job
_response=$(databricks jobs ${task} --json '
{
    "name": "'${_job_name}'",
    "schedule": {
        "quartz_cron_expression": "0 15 10 * * ? *",
        "timezone_id": "UTC",
        "pause_status": "PAUSED"
    },
    "max_concurrent_runs": 1,
    "job_clusters": [
      {
        "job_cluster_key": "shared_cluster",
            "new_cluster": {
                "cluster_name": "",
                "spark_version": "9.1.x-scala2.12",
                "spark_conf": {
                    "spark.master": "local[*, 4]",
                    "spark.databricks.cluster.profile": "singleNode",
                    "spark.databricks.delta.preview.enabled": "true"
                },
                "azure_attributes": {
                    "availability": "ON_DEMAND_AZURE"
                },
                "node_type_id": "Standard_F4s_v2",
                "custom_tags": {
                    "ResourceClass": "SingleNode"
                },
                "enable_elastic_disk": true,
                "num_workers": 0
            }
        }
    ],
    "tasks": [
        {
            "task_key": "00-setup",
            "description": "Setup",
            "job_cluster_key": "shared_cluster",
            "notebook_task": {
                "notebook_path": "'${_notebooks_path}'/setup",
                "base_parameters": {
                    "BasePath": "'${_data_path}'",
                    "DatabaseName": "'${_database_name}'"
                }
            },
            "timeout_seconds": 86400
        },
        {
            "task_key": "01-overdoses-bronze",
            "description": "Bronze - Overdoses",
            "job_cluster_key": "shared_cluster",
            "notebook_task": {
                "notebook_path": "'${_notebooks_path}'/01-overdoses-bronze",
                "base_parameters": {
                    "BasePath": "'${_data_path}'",
                    "DatabaseName": "'${_database_name}'"
                }
            },
            "timeout_seconds": 86400,
            "depends_on": [
                {
                    "task_key": "00-setup"
                }
            ]
        },
        {
            "task_key": "02-overdoses-silver",
            "description": "Silver - Overdoses",
            "job_cluster_key": "shared_cluster",
            "notebook_task": {
                "notebook_path": "'${_notebooks_path}'/02-overdoses-silver",
                "base_parameters": {
                    "BasePath": "'${_data_path}'",
                    "DatabaseName": "'${_database_name}'"
                }
            },
            "timeout_seconds": 86400,
            "depends_on": [
                {
                    "task_key": "01-overdoses-bronze"
                }
            ]
        },
        {
            "task_key": "01-world-bank-bronze",
            "description": "Bronze - World Bank",
            "job_cluster_key": "shared_cluster",
            "notebook_task": {
                "notebook_path": "'${_notebooks_path}'/01-world-bank-bronze",
                "base_parameters": {
                    "BasePath": "'${_data_path}'",
                    "DatabaseName": "'${_database_name}'"
                }
            },
    				"libraries": [
    				    {
    					    	"whl": "dbfs:'${_libs_path}'/lakehousePipelines-'${_libs_version}'-py3-none-any.whl"
    				    },
    				    {
    				  	  	"whl": "dbfs:'${_libs_path}'/lakehouseLibs-'${_libs_version}'-py3-none-any.whl"
    				    }
    				],
            "timeout_seconds": 86400,
            "depends_on": [
                {
                    "task_key": "00-setup"
                }
            ]
        },
        {
            "task_key": "02-world-bank-silver",
            "description": "Silver - World Bank",
            "job_cluster_key": "shared_cluster",
            "notebook_task": {
                "notebook_path": "'${_notebooks_path}'/02-world-bank-silver",
                "base_parameters": {
                    "BasePath": "'${_data_path}'",
                    "DatabaseName": "'${_database_name}'"
                }
            },
    				"libraries": [
    				    {
    					    	"whl": "dbfs:'${_libs_path}'/lakehousePipelines-'${_libs_version}'-py3-none-any.whl"
    				    },
    				    {
    				  	  	"whl": "dbfs:'${_libs_path}'/lakehouseLibs-'${_libs_version}'-py3-none-any.whl"
    				    }
    				],
    				"timeout_seconds": 86400,
            "depends_on": [
                {
                    "task_key": "01-world-bank-bronze"
                }
            ]
        },
        {
            "task_key": "01-who-bronze",
            "description": "Bronze - World Health Organization",
            "job_cluster_key": "shared_cluster",
            "python_wheel_task": {
                "package_name": "lakehousePipelines",
                "entry_point": "run_who_bronze",
                "named_parameters": {
                    "basePath": "'${_data_path}'"
                }
            },
    				"libraries": [
    				    {
    					    	"whl": "dbfs:'${_libs_path}'/lakehousePipelines-'${_libs_version}'-py3-none-any.whl"
    				    },
    				    {
    				  	  	"whl": "dbfs:'${_libs_path}'/lakehouseLibs-'${_libs_version}'-py3-none-any.whl"
    				    }
    				],
    			  "timeout_seconds": 86400,
            "depends_on": [
                {
                    "task_key": "00-setup"
                }
            ]
        },
        {
            "task_key": "02-who-silver",
            "description": "Silver - World Health Organization",
            "job_cluster_key": "shared_cluster",
            "python_wheel_task": {
                "package_name": "lakehousePipelines",
                "entry_point": "run_who_silver",
                "named_parameters": {
                    "basePath": "'${_data_path}'",
                    "databaseName": "'${_database_name}'"
                }
            },
    				"libraries": [
    				    {
    					    	"whl": "dbfs:'${_libs_path}'/lakehousePipelines-'${_libs_version}'-py3-none-any.whl"
    				    },
    				    {
    				  	  	"whl": "dbfs:'${_libs_path}'/lakehouseLibs-'${_libs_version}'-py3-none-any.whl"
    				    }
    				],
    			  "timeout_seconds": 86400,
            "depends_on": [
                {
                    "task_key": "01-who-bronze"
                }
            ]
        },
        {
            "task_key": "03-indicators-gold",
            "description": "Gold - Health Indicators",
            "job_cluster_key": "shared_cluster",
            "notebook_task": {
                "notebook_path": "'${_notebooks_path}'/03-indicators-gold",
                "base_parameters": {
                    "BasePath": "'${_data_path}'",
                    "DatabaseName": "'${_database_name}'"
                }
            },
            "timeout_seconds": 86400,
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
    "format": "MULTI_TASK",
    "timeout_seconds": 0,
    "email_notifications": {}
}'
)

if [ -z "$job_id" ];
then
  job_id=$(echo "${_response}" | python -c 'import sys,json; print(json.load(sys.stdin)["job_id"])' 2> /dev/null)
fi

[ -z "${job_id}" ] && { echo "${_response}"; exit 1; }
echo "${job_id}"
