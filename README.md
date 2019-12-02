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
                         --job_name JOB-NAME \
                         --start_date YYYY-MM-DD \
                         --end_date YYYY-MM-DD \
                         --input_dataset INPUT_DATASET \
                         --output_table OUTPUT_DATASET_NAME.OUTPUT_TABLE_NAME \
                         --config anchorage_cfg.yaml \
                         --max_num_workers MAX_WORKER \
                         --fishing_ssvid_list GCS_LOCATION_OF_FISHING_SSVID_LIST \
                         --project PROJECT-NAME \
                         --requirements_file requirements.txt \
                         --staging_location GCS_STAGING_LOCATION \
                         --temp_location GCS_TEMPLOCATION \
                         --setup_file ./setup.py \
                         --runner DataflowRunner \
                         --worker_machine_type=custom-1-13312-ext \
                         --disk_size_gb 200


Standard dataflow options must also be specified.

For example, to run all years:

    docker-compose run anchorages \
                         --job_name unnamed-anchorages \
                         --start_date 2012-01-01 \
                         --end_date 2019-06-30 \
                         --input_dataset pipe_production_b \
                         --output_table machine_learning_dev_ttl_120d.unnamed_anchorages_v20190816 \
                         --config anchorage_cfg.yaml \
                         --max_num_workers 300 \
                         --fishing_ssvid_list gs://machine-learning-dev-ttl-120d/fishing_mmsi.txt \
                         --project world-fishing-827 \
                         --requirements_file requirements.txt \
                         --project world-fishing-827 \
                         --staging_location gs://machine-learning-dev-ttl-120d/anchorages/anchorages/output/staging \
                         --temp_location gs://machine-learning-dev-ttl-120d/anchorages/temp \
                         --setup_file ./setup.py \
                         --runner DataflowRunner \
                         --worker_machine_type=custom-1-13312-ext \
                         --disk_size_gb 200

Or to run a minimal testing run:

    docker-compose run anchorages \
                         --job_name unnamed-anchorages \
                         --start_date 2017-01-01 \
                         --end_date 2017-06-30 \
                         --input_dataset pipe_production_b \
                         --output_table machine_learning_dev_ttl_120d.unnamed_anchorages_test \
                         --config anchorage_cfg.yaml \
                         --max_num_workers 300 \
                         --fishing_ssvid_list gs://machine-learning-dev-ttl-120d/fishing_mmsi.txt \
                         --project world-fishing-827 \
                         --requirements_file requirements.txt \
                         --project world-fishing-827 \
                         --staging_location gs://machine-learning-dev-ttl-120d/anchorages/anchorages/output/staging \
                         --temp_location gs://machine-learning-dev-ttl-120d/anchorages/temp \
                         --setup_file ./setup.py \
                         --runner DataflowRunner \
                         --worker_machine_type=custom-1-13312-ext \
                         --disk_size_gb 200

*Note that `fishing_ssvid_list` should refer to a file on GCS.*


### Naming Anchorage Points

After a set of anchorages is created, names are assigned using `name_anchorages_main`

For example:

    docker-compose run name_anchorages \
                 --job_name name-anchorages \
                 --input_table machine_learning_dev_ttl_120d.unnamed_anchorages_test \
                 --output_table machine_learning_dev_ttl_120d.named_anchorages_test \
                 --config ./name_anchorages_cfg.yaml \
                 --max_num_workers 200 \
                 --fishing_ssvid_list gs://machine-learning-dev-ttl-120d/fishing_mmsi.txt \
                 --project world-fishing-827 \
                 --requirements_file requirements.txt \
                 --project world-fishing-827 \
                 --staging_location gs://machine-learning-dev-ttl-120d/anchorages/anchorages/output/staging \
                 --temp_location gs://machine-learning-dev-ttl-120d/anchorages/temp \
                 --setup_file ./setup.py \
                 --runner DataflowRunner \
                 --disk_size_gb 100

or

    docker-compose run name_anchorages \
                 --job_name name-anchorages \
                 --input_table machine_learning_dev_ttl_120d.unnamed_anchorages_v20190816 \
                 --output_table machine_learning_dev_ttl_120d.named_anchorages_v20180827 \
                 --config ./name_anchorages_cfg.yaml \
                 --max_num_workers 200 \
                 --fishing_ssvid_list gs://machine-learning-dev-ttl-120d/fishing_mmsi.txt \
                 --project world-fishing-827 \
                 --requirements_file requirements.txt \
                 --project world-fishing-827 \
                 --staging_location gs://machine-learning-dev-ttl-120d/anchorages/anchorages/output/staging \
                 --temp_location gs://machine-learning-dev-ttl-120d/anchorages/temp \
                 --setup_file ./setup.py \
                 --runner DataflowRunner \
                 --disk_size_gb 100

The override path points to a csv file containing anchorages that are either missing or incorrectly named.
It should have the following fields: s2uid,label,iso3,anchor_lat,anchor_lon,sublabel.


### Updating Port Events


#### Manually

To update a day of events, run, for example:

    docker-compose run port_events \
        --job_name porteventstest \
        --input_table pipe_production_b.position_messages_ \
        --anchorage_table gfw_research.named_anchorages_v20190307 \
        --start_date 2017-01-01 \
        --end_date 2017-12-31 \
        --output_table machine_learning_dev_ttl_120d.new_pipeline_port_events_test_v20180408_ \
        --project world-fishing-827 \
        --max_num_workers 200 \
        --requirements_file requirements.txt \
        --project world-fishing-827 \
        --staging_location gs://machine-learning-dev-ttl-30d/anchorages/portevents/output/staging \
        --temp_location gs://machine-learning-dev-ttl-30d/anchorages/temp \
        --setup_file ./setup.py \
        --runner DataflowRunner \
        --disk_size_gb 100


For a full list of options run:

    python -m port_events -h


To create a corresponding day of visits do:

    docker-compose run port_visits \
        --job_name portvisitssharded \
        --events_table machine_learning_dev_ttl_30d.new_pipeline_port_events_test \
        --start_date 2017-12-01 \
        --end_date 2017-12-01 \
        --start_padding 365 \
        --output_table machine_learning_dev_ttl_30d.new_pipeline_port_visits_test \
        --project world-fishing-827 \
        --max_num_workers 200 \
        --requirements_file requirements.txt \
        --project world-fishing-827 \
        --staging_location gs://machine-learning-dev-ttl-30d/anchorages/portevents/output/staging \
        --temp_location gs://machine-learning-dev-ttl-30d/anchorages/temp \
        --setup_file ./setup.py \
        --runner DataflowRunner \
        --disk_size_gb 100 


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

# License

Copyright 2017 Global Fishing Watch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
