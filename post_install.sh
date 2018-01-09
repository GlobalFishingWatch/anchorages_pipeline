python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force DOCKER_IMAGE=$1 \
    PIPE_ANCHORAGES \
    PORT_EVENTS_ANCHORAGE_TABLE=gfw_raw.anchorage_naming_20171026 \
    PORT_EVENTS_INPUT_TABLE=pipe_test_b_ttl30.position_messages_ \
    PORT_EVENTS_OUTPUT_TABLE=machine_learning_dev_ttl_30d.new_pipeline_port_events_ \
    PORT_EVENTS_START_DATE=$(date --date="7 days ago" +"%Y-%m-%d") \
    PORT_VISITS_OUTPUT_TABLE=machine_learning_dev_ttl_30d.new_pipeline_port_visits_ \
    PORT_VISITS_START_PADDING=365 \
    PORT_VISITS_START_DATE=$(date --date="7 days ago" +"%Y-%m-%d") \
    GCS_BUCKET=machine-learning-dev-ttl-30d/anchorages

echo "Installation Complete"
