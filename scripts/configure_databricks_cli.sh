#!/usr/bin/env bash
#
# Copy files to DBFS
#

# Required parameters
_workspace_url=${1}
_access_token=${2}
_cli_profile=${3}

_usage() {
  echo -e "Usage: ${0} <_workspace_url> <_access_token>"
  exit 1
}

# Parameters check
if [ -z "${_workspace_url}" ]; then
  _workspace_url=$DATABRICKS_HOST
fi
[ -z "${_workspace_url}" ] && _usage

if [ -z "${_access_token}" ]; then
  _access_token=$DATABRICKS_TOKEN
fi
[ -z "${_access_token}" ] && _usage

if [ -z "${_cli_profile}" ]; then
  _cli_profile=DEFAULT
fi

# Install the databricks-cli
pip install databricks-cli --upgrade

# Configure the databricks-cli
_sed="sed"
[[ $OSTYPE == 'darwin'* ]] && _sed="gsed"
${_sed} -i "/\[${_cli_profile}\]/{N;N;d}" ~/.databrickscfg
echo "[${_cli_profile}]" >> ~/.databrickscfg
echo "host = ${_workspace_url}" >> ~/.databrickscfg
echo "token = ${_access_token}" >> ~/.databrickscfg

# Test
databricks --profile "${DATABRICKS_CLI_PROFILE}" workspace ls
