#!/usr/bin/env bash
#
# Copy files to DBFS
#

# Required parameters
_src_path=${1}
_dest_dbfs=${2}
_cli_profile=${3}

_usage() {
  echo -e "Usage: ${0} <src_file> <dest_dbfs>"
  exit 1
}

# Parameters check
[ -z "${_src_path}" ] && _usage
[ -z "${_dest_dbfs}" ] && _usage
if [ -z "${_cli_profile}" ]; then
  _cli_profile=DEFAULT
fi

file_name=$(basename "${_src_path}")
databricks --profile "${_cli_profile}" fs cp --recursive --overwrite "${_src_path}" "${_dest_dbfs}/${file_name}"
echo "${_dest_dbfs}/${file_name}"
