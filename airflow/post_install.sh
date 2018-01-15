python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force DOCKER_IMAGE=$1 \
    PIPE_ANCHORAGES \
    PORT_VISITS_START_PADDING=365 \
    PORT_VISITS_START_DATE=$(date --date="7 days ago" +"%Y-%m-%d") \
    PORT_EVENTS_ANCHORAGE_TABLE=gfw_raw.anchorage_naming_20171026 \
    PORT_EVENTS_INPUT_TABLE="{{ PIPELINE_DATASET }}.position_messages_" \
    PORT_EVENTS_OUTPUT_TABLE="{{ PIPELINE_DATASET }}.port_events_" \
    PORT_VISITS_OUTPUT_TABLE="{{ PIPELINE_DATASET }}.port_visits_" 

echo "Installation Complete"
