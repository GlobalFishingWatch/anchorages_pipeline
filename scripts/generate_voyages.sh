#!/bin/bash
set -e
THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
source pipe-tools-utils
source ${THIS_SCRIPT_DIR}/pipeline.sh

PROCESS=$(basename $0 .sh)
ARGS=( SOURCE_TABLE MIN_CONFIDENCE DEST_TABLE )

display_usage() {
  echo -e "\nUsage:\n${PROCESS}.sh ${ARGS[*]}\n"
  echo -e "SOURCE_TABLE: The port visits table (format: PROJECT:DATASET.TABLE).\n"
  echo -e "MIN_CONFIDENCE: The minimal confidence to get the voyages from port visits table (format: Number).\n"
  echo -e "DEST_TABLE: The voyages table (format: PROJECT:DATASET.TABLE).\n"
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

SCHEMA=${ASSETS}/${PROCESS}.schema.json
SQL=${ASSETS}/${PROCESS}.sql.j2
TABLE_DESC=(
  "Table of all voyages. One row per voyage"
  ""
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${SOURCE_TABLE}"
  "* Command: $(basename $0)"
)
TABLE_DESC+=(${PARAMS[*]})
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )


############################################################
# Run query to generate voyages.
############################################################
PARTITION_BY_ID="trip_end"
CLUSTER_BY="ssvid,vessel_id,trip_start,trip_id"
echo "Publishing ${PROCESS} to ${DEST_TABLE}..."
echo ""
echo "Table Description"
echo "${TABLE_DESC}"
echo ""
echo "Executing query..."
SQL=$(jinja2 ${SQL} -D port_visits_table=${SOURCE_TABLE//:/.} -D min_confidence=${MIN_CONFIDENCE})
echo "${SQL}" \
  | bq query \
    --headless \
    --max_rows=0 \
    --allow_large_results \
    --replace \
    --time_partitioning_type=DAY \
    --time_partitioning_field="${PARTITION_BY_ID}" \
    --clustering_fields "${CLUSTER_BY}" \
    --destination_table ${DEST_TABLE}

if [ "$?" -ne 0 ]; then
  echo "  Unable to run the voyage generation query."
  exit 1
else
  echo "  The voyage generation query has RUN."
fi

############################################################
# Updates the description of the table.
############################################################
echo "Updating the desciption and schema of the table."
bq update \
  --schema ${SCHEMA} \
  --description "${TABLE_DESC}" \
  ${DEST_TABLE}
if [ "$?" -ne 0 ]; then
  echo "  Unable to update the voyages table decription."
  exit 1
else
  echo "  ${DEST_TABLE} Updated."
fi
echo
echo "DONE."
