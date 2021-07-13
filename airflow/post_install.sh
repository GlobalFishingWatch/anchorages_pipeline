#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"


python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_anchorages \
    dag_install_path="${THIS_SCRIPT_DIR}" \
    dataflow_runner="DataflowRunner" \
    docker_run="{{ var.value.DOCKER_RUN }}" \
    project_id="{{ var.value.PROJECT_ID }}" \
    temp_bucket="{{ var.value.TEMP_BUCKET }}"  \
    pipeline_bucket="{{ var.value.PIPELINE_BUCKET }}" \
    pipeline_dataset="{{ var.value.PIPELINE_DATASET }}" \
    anchorage_table="anchorages.named_anchorages_v20201104" \
    source_dataset="{{ var.value.PIPELINE_DATASET }}" \
    source_table="position_messages_" \
    port_events_table="port_events_" \
    port_visits_table="port_visits_" \
    voyages_table="voyages" \
    dataflow_max_num_workers="100" \
    dataflow_disk_size_gb="50" \
    region='us-central1'


echo "Installation Complete"
