#!/bin/bash
set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

display_usage() {
	echo -e "\nUsage:\npublish_events YYYY-MM-DD[,YYYY-MM-DD] SOURCE_TABLE DEST_TABLE \n"
	}


if [[ $# -ne 3  ]]
then
    display_usage
    exit 1
fi

DATE_RANGE=$1
SOURCE_TABLE=$2
DEST_TABLE=$3

IFS=, read START_DATE END_DATE <<<"${DATE_RANGE}"
if [[ -z $END_DATE ]]; then
  END_DATE=${START_DATE}
fi


DELETE_SQL=${ASSETS}/port-events-delete.sql.j2
INSERT_SQL=${ASSETS}/port-events-insert.sql.j2
SCHEMA=${ASSETS}/events.schema.json
TABLE_DESC=(
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${SOURCE_TABLE}"
  "* Command:"
  "$(basename $0)"
  "$@"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )


echo "Publishing port in/out events to ${DEST_TABLE}..."
echo "${TABLE_DESC}"

echo "  Create table"

bq mk --force \
  --description "${TABLE_DESC}" \
  --schema ${SCHEMA} \
  --time_partitioning_field=timestamp \
  ${DEST_TABLE}

echo "  Deleting existing records for ${START_DATE} to ${END_DATE}"

jinja2 ${DELETE_SQL} -D table=${DEST_TABLE//:/.} -D start_date=${START_DATE} -D end_date=${END_DATE} \
     | bq query --max_rows=0

echo "  Inserting new records for ${START_DATE} to ${END_DATE}"

jinja2 ${INSERT_SQL} \
   -D source=${SOURCE_TABLE//:/.} \
   -D dest=${DEST_TABLE//:/.} \
   -D start_yyyymmdd=$(yyyymmdd ${START_DATE}) \
   -D end_yyyymmdd=$(yyyymmdd ${END_DATE}) \
   | bq query --max_rows=0

echo "  Updating table description ${DEST_TABLE}"

#bq update --description "${TABLE_DESC}" ${DEST_TABLE}

echo "  ${DEST_TABLE} Done."


