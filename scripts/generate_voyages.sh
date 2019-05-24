#!/bin/bash
set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

PROCESS=$(basename $0 .sh)
ARGS=( DATASET PORT_VISITS_TABLE DEST_TABLE )
SCHEMA=${ASSETS}/${PROCESS}.schema.json
SQL=${ASSETS}/${PROCESS}.sql.j2
TABLE_DESC=(
  "Table of all voyages. One row per voyage"
  ""
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${DATASET}.${PORT_VISITS_TABLE}"
  "* Command: $(basename $0)"
)

display_usage()
{
  echo -e "\nUsage:\n${PROCESS}.sh ${ARGS[*]} \n"
}

if [[ $# -ne ${#ARGS[@]} ]]
then
    display_usage
    exit 1
fi

ARG_VALUES=("$@")
PARAMS=()
for index in ${!ARGS[*]}; do
  echo ${ARG_VALUES[$index]}
  declare "${ARGS[$index]}"="${ARG_VALUES[$index]}"
  PARAMS+=("${ARGS[$index]}=${ARG_VALUES[$index]}")
done

TABLE_DESC+=(${PARAMS[*]})
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )


echo "Publishing ${PROCESS} to ${DEST_TABLE}..."
echo ""
echo "Table Description"
echo "${TABLE_DESC}" 
echo ""
echo "Executing query..."
SQL=$(jinja2 ${SQL} -D dataset=${DATASET//:/.} -D port_visits_table=${PORT_VISITS_TABLE})
echo "${SQL}" | bq query \
    --headless \
    --max_rows=0 \
    --allow_large_results \
    --replace \
    --destination_table ${DEST_TABLE}

if [ "$?" -ne 0 ]; then
  echo "  Unable to run the voyage generation query."
  exit 1
fi

echo ""
bq update --schema ${SCHEMA} --description "${TABLE_DESC}" ${DEST_TABLE}

if [ "$?" -ne 0 ]; then
  echo "  Unable to update the voyages table decription."
  exit 1
fi

echo ""
echo "DONE ${DEST_TABLE}."
