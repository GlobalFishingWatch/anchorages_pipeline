# Anchorages

## Creating Anchorage points

Run:
  
    python -m anchorages_main --name updateanchorages \
                              --start-date 2012-01-01 \
                              --end-date 2017-12-31 \
                              --output BQ_TABLE_NAME


## Updating Port Visits

To update a single day of visits, run:

    python -m port_visits_main --name JOB_NAME \
                               --anchorage-path GS_PATH_TO_ANCHORAGES \
                               --start-date YYYY-MM-DD \
                               --end-date YYYY-MM-DD 
                               --output BQ_TABLE_NAME

For example:

    python -m port_visits_main --name testvisits \
                               --anchorage-path gs://machine-learning-dev-ttl-30d/anchorages.json  \
                               --start-date 2016-02-01 \
                               --end-date 2016-02-01 \
                               --output machine_learning_dev_ttl_30d.testvisits

Results are **appended** to the specified file.

For a full list of options run:

    python -m port_visits_main -h


## Config file

Parameters controlling the generation of anchorages and port_visits is stored
in a `.yaml` file. By default this information is read from `config.yaml`, but
a different configuration can be specified use the `--config` flag.

