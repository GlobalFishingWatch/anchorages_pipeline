#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"


python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_anchorages \
    dag_install_path="${THIS_SCRIPT_DIR}" \
    dataflow_runner="DataflowRunner" \
    docker_run="{{ var.value.DOCKER_RUN }}" \
    events_dataset="{{ var.value.EVENTS_DATASET }}" \
    events_table="port_events" \
    project_id="{{ var.value.PROJECT_ID }}" \
    temp_bucket="{{ var.value.TEMP_BUCKET }}"  \
    pipeline_bucket="{{ var.value.PIPELINE_BUCKET }}" \
    pipeline_dataset="{{ var.value.PIPELINE_DATASET }}" \
    port_visits_start_padding=365 \
    anchorage_table=gfw_raw.anchorage_naming_20171026 \
    source_dataset="{{ var.value.PIPELINE_DATASET }}" \
    source_table="position_messages_" \
    port_events_table="port_events_" \
    port_visits_table="port_visits_"


echo "Installation Complete"
