#!/usr/bin/env bash

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_anchorages \
    docker_run="{{ var.value.DOCKER_RUN }}" \
    project_id="{{ var.value.PROJECT_ID }}" \
    temp_bucket="{{ var.value.TEMP_BUCKET }}"  \
    pipeline_bucket="{{ var.value.PIPELINE_BUCKET }}" \
    pipeline_dataset="{{ var.value.PIPELINE_DATASET }}" \
    port_visits_start_padding=365 \
    port_events_start_date=$(date --date="7 days ago" +"%Y-%m-%d") \
    port_visits_start_date=$(date --date="7 days ago" +"%Y-%m-%d") \
    anchorage_table=gfw_raw.anchorage_naming_20171026 \
    source_dataset="{{ var.value.PIPELINE_DATASET }}" \
    source_table="position_messages_" \
    port_events_table="port_events_" \
    port_visits_table="port_visits_"


echo "Installation Complete"
