echo "Installation Complete"

python $AIRFLOW_HOME/utils/set_default_variables.py PIPE_ANCHORAGES \
    PORT_EVENTS_ANCHORAGE_TABLE=gfw_raw.anchorage_naming_20171026 \
    PORT_EVENTS_INPUT_TABLE=pipeline_classify_p_p516_daily \
    PORT_EVENTS_OUTPUT_TABLE=machine_learning_dev_ttl_30d.in_out_events_test \
    GCS_BUCKET=machine-learning-dev-ttl-30d/anchorages \
    DOCKER_IMAGE=$1
