# Anchorages

This repository contains pipelines for finding anchorages and associated port-visit events.

# Running

## Dependencies

You just need [docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) in your machine to run the
pipeline. No other dependency is required.

## Setup

The pipeline reads it's input from BigQuery, so you need to first authenticate
with your google cloud account inside the docker images. To do that, you need
to run this command and follow the instructions:

```
docker-compose run gcloud auth application-default login
```

## CLI

The pipeline includes a CLI that can be used to start both local test runs and
remote full runs. Just run `docker-compose run [anchorages|name_anchorages|port_events] --help` and follow the
instructions there.


### Creating Anchorage Points


Run:
  
    docker-compose run anchorages \
                         --name updateanchorages \
                         --start-date YYYY-MM-DD \
                         --end-date YYYY-MM-DD \
                         --output BQ_TABLE_NAME \
                         --input-table INPUT_TABLE \
                         --fishing-mmsi-list FISHING_LIST


Standard dataflow options can also be specified.

For example, to run all years:

    docker-compose run anchorages \
                         --name anchoragesallyears \
                         --start-date 2012-01-01 \
                         --end-date 2017-12-31 \
                         --input-table pipeline_classify_p_p516_daily \
                         --max_num_workers 200 \
                         --fishing-mmsi-list fishing_list.txt

Or to run a minimal testing run:

    docker-compose run anchorages \
                         --name testanchorages2016tiny \
                         --start-date 2016-01-01 \
                         --end-date 2016-01-31 \
                         --input-table pipeline_classify_p_p516_daily \
                         --fishing-mmsi-list fishing_list.txt

*Note that `FISHING_LIST` must be a local file or docker will not be able to see it, so copy it to your local directory before launching.*


### Naming Anchorage Points

After a set of anchorages is created, names are assigned using `name_anchorages_main`

For example:

    docker-compose run name_anchorages \
                  --name nameanchorages \
                  --input-table gfw_raw.unnamed_anchorages \
                  --output-table machine_learning_dev_ttl_30d.named_anchorages_test \
                  --config-path ./name_anchorages_cfg.yaml


The override path points to a csv file containing anchorages that are either missing or incorrectly named.
It should have the following fields: s2uid,label,iso3,anchor_lat,anchor_lon,sublabel.


### Updating Port Events

#### Using Airflow

Install airflow and setup using the instructions at https://github.com/GlobalFishingWatch/pipe-model/tree/18-pipe-classify-ident/issue-18-airflow.

Create a link in the `dags` directory of your airflow setup to `port_events_dag.py`:

    ln [PATH TO ANCHORAGES]/port_events_dag.py [PATH TO AIRFLOW]/dags/port_events_dag.py

Define the following variables in your airflow distribution:

- **PORT_EVENTS_ANCHORAGE_TABLE**
- **PORT_EVENTS_INPUT_TABLE**
- **PORT_EVENTS_OUTPUT_TABLE**


#### Manually

To update a single day of events, run:

    docker-compose run port_events \
                          --name JOB_NAME \
                          --anchorage-path GS_PATH_TO_ANCHORAGES \
                          --start-date YYYY-MM-DD \
                          --end-date YYYY-MM-DD 
                          --output BQ_TABLE_NAME

For example:

    docker-compose run port_events \
                          --job_name portvisitsoneday \
                          --anchorage-table gfw_raw.anchorage_naming_20171026 \
                          --start-date 2016-01-01 \
                          --end-date 2016-01-01 \
                          --output-table machine_learning_dev_ttl_30d.in_out_events_test \
                          --project world-fishing-827 \
                          --max_num_workers 100

Results are **appended** to the specified file.

For a full list of options run:

    python -m port_events -h


### Config file

Parameters controlling the generation of anchorages and port_visits is stored
in a `.yaml` file. By default this information is read from `config.yaml`, but
a different configuration can be specified use the `--config` flag.


## Code / Data Updates:

Please perform the following steps.

1. Branch the git repo.
2. Create a pull request (PR).
3. Add description of changes with a link to the PR in `CHANGES.md`.  In some
   cased multiple PRs may be involved in a single version change; list them all.
4. Bump version in VERSION.
5. Merge PR after review. Note that if using squash merge, you may need
   to fiddle with the commit references.
