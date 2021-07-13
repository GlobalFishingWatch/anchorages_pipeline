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

### Updating the Named Anchorages

The most common manual task is updating the named anchorages, which needs to be done whenever
anchorage overrides is edited. This is accomplished by running the following command:

    docker-compose run name_anchorages \
                 --job_name name-anchorages \
                 --input_table anchorages.CURRENT_UNNAMED_ANCHORAGES \
                 --output_table TARGET_DATASET.TARGET_TABLE \
                 --config ./name_anchorages_cfg.yaml \
                 --max_num_workers 100 \
                 --fishing_ssvid_list gs://machine-learning-dev-ttl-120d/fishing_mmsi.txt \
                 --project world-fishing-827 \
                 --requirements_file requirements-worker-frozen.txt \
                 --project world-fishing-827 \
                 --staging_location gs://machine-learning-dev-ttl-120d/anchorages/anchorages/output/staging \
                 --temp_location gs://machine-learning-dev-ttl-120d/anchorages/temp \
                 --setup_file ./setup.py \
                 --runner DataflowRunner \
                 --disk_size_gb 100

where `CURRENT_UNNAMED_ANCHORAGES` is the current (typically most recent) unnamed anchorages
table and `TARGET_DATASET.TARGET_TABLE` is where the unnamed anchorages are stored.  I often
put this in a temporary table for inspection, then copy it to it's final destination.


### Creating Anchorage Points


Run:
  
    docker-compose run anchorages \
                         --job_name JOB-NAME \
                         --start_date YYYY-MM-DD \
                         --end_date YYYY-MM-DD \
                         --messages_thinned_table DATASET.messages_thinned_ \
                         --output_table OUTPUT_DATASET_NAME.OUTPUT_TABLE_NAME \
                         --config anchorage_cfg.yaml \
                         --max_num_workers MAX_WORKER \
                         --fishing_ssvid_list GCS_LOCATION_OF_FISHING_SSVID_LIST \
                         --project PROJECT-NAME \
                         --requirements_file requirements-worker-frozen.txt \
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
                         --messages_thinned_table pipe_production_b.messages_thinned_ \
                         --output_table machine_learning_dev_ttl_120d.unnamed_anchorages_v20190816 \
                         --config anchorage_cfg.yaml \
                         --max_num_workers 300 \
                         --fishing_ssvid_list gs://machine-learning-dev-ttl-120d/fishing_mmsi.txt \
                         --project world-fishing-827 \
                         --requirements_file requirements-worker-frozen.txt \
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
                         --end_date 2017-01-31 \
                         --messages_thinned_table machine_learning_dev_ttl_120d.messages_segmented_ \
                         --output_table machine_learning_dev_ttl_120d.unnamed_anchorages_test \
                         --config anchorage_cfg.yaml \
                         --max_num_workers 200 \
                         --fishing_ssvid_list gs://machine-learning-dev-ttl-120d/fishing_mmsi.txt \
                         --project world-fishing-827 \
                         --requirements_file requirements-worker-frozen.txt \
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
                 --requirements_file requirements-worker-frozen.txt \
                 --project world-fishing-827 \
                 --staging_location gs://machine-learning-dev-ttl-120d/anchorages/anchorages/output/staging \
                 --temp_location gs://machine-learning-dev-ttl-120d/anchorages/temp \
                 --setup_file ./setup.py \
                 --runner DataflowRunner \
                 --disk_size_gb 100

or

    docker-compose run name_anchorages \
                 --job_name name-anchorages \
                 --input_table anchorages.unnamed_anchorages_v20190816 \
                 --output_table machine_learning_dev_ttl_120d.named_anchorages_v20210429 \
                 --config ./name_anchorages_cfg.yaml \
                 --max_num_workers 100 \
                 --fishing_ssvid_list gs://machine-learning-dev-ttl-120d/fishing_mmsi.txt \
                 --project world-fishing-827 \
                 --requirements_file requirements-worker-frozen.txt \
                 --project world-fishing-827 \
                 --staging_location gs://machine-learning-dev-ttl-120d/anchorages/anchorages/output/staging \
                 --temp_location gs://machine-learning-dev-ttl-120d/anchorages/temp \
                 --setup_file ./setup.py \
                 --runner DataflowRunner \
                 --disk_size_gb 100 \
                 --region us-central1


### Updating Port Events


#### Manually

To update a day of events, run, for example:

   docker-compose run port_events \
        --job_name porteventstest \
        --input_table pipe_production_v20201001.position_messages_ \
        --anchorage_table anchorages.named_anchorages_v20201104 \
        --start_date 2018-01-01 \
        --end_date 2018-12-31 \
        --output_table machine_learning_dev_ttl_120d.raw_port_events_v20210506_ \
        --state_table machine_learning_dev_ttl_120d.port_port_state_v20210506_ \
        --project world-fishing-827 \
        --max_num_workers 100 \
        --requirements_file requirements-worker-frozen.txt \
        --project world-fishing-827 \
        --staging_location gs://machine-learning-dev-ttl-30d/anchorages/portevents/output/staging \
        --temp_location gs://machine-learning-dev-ttl-30d/anchorages/temp \
        --setup_file ./setup.py \
        --runner DataflowRunner \
        --disk_size_gb 100 \
        --region us-central1 \
        --ssvid_filter '(select case(vi_ssvid as string) from machine_learning_dev_ttl_120d.vessel_list_new_visits_5_6_21)'

    docker-compose run port_events \
            --job_name porteventstest \
            --input_table pipe_production_v20201001.position_messages_ \
            --anchorage_table anchorages.named_anchorages_v20201104 \
            --start_date 2017-01-01 \
            --end_date 2021-4-30 \
            --output_table machine_learning_dev_ttl_120d.port_event_test_v20210506_events_ \
            --state_table machine_learning_dev_ttl_120d.port_event__test_v20210506_batch_state_ \
            --project world-fishing-827 \
            --max_num_workers 100 \
            --requirements_file requirements-worker-frozen.txt \
            --project world-fishing-827 \
            --staging_location gs://machine-learning-dev-ttl-30d/anchorages/portevents/output/staging \
            --temp_location gs://machine-learning-dev-ttl-30d/anchorages/temp \
            --setup_file ./setup.py \
            --runner DataflowRunner \
            --disk_size_gb 100 \
            --region us-central1

For a full list of options run:

    python -m port_events -h


To create a corresponding day of visits do:

    docker-compose run port_visits \
        --job_name portvisitstest \
        --events_table machine_learning_dev_ttl_120d.raw_port_events_v20210506_ \
        --vessel_id_table pipe_production_v20201001.segment_info \
        --bad_segs_table "(SELECT DISTINCT seg_id FROM world-fishing-827.gfw_research.pipe_v20201001_segs WHERE overlapping_and_short)" \
        --start_date 2018-01-01 \
        --end_date 2018-12-31 \
        --output_table machine_learning_dev_ttl_120d.port_visit_test_v20210506_stableid \
        --project world-fishing-827 \
        --max_num_workers 50 \
        --requirements_file requirements-worker-frozen.txt \
        --project world-fishing-827 \
        --staging_location gs://machine-learning-dev-ttl-30d/anchorages/portevents/output/staging \
        --temp_location gs://machine-learning-dev-ttl-30d/anchorages/temp \
        --setup_file ./setup.py \
        --runner DataflowRunner \
        --disk_size_gb 100 \
        --region us-central1



    docker-compose run port_visits \
            --job_name portvisitstest \
            --events_table machine_learning_dev_ttl_120d.port_event_test_v20210506_events_ \
            --vessel_id_table pipe_production_v20201001.segment_info \
            --bad_segs_table "(SELECT DISTINCT seg_id FROM world-fishing-827.gfw_research.pipe_v20201001_segs WHERE overlapping_and_short)" \
            --start_date 2017-01-01 \
            --end_date 2021-04-30 \
            --output_table machine_learning_dev_ttl_120d.port_visit_test_v20210506_stableid \
            --project world-fishing-827 \
            --max_num_workers 50 \
            --requirements_file requirements-worker-frozen.txt \
            --project world-fishing-827 \
            --staging_location gs://machine-learning-dev-ttl-30d/anchorages/portevents/output/staging \
            --temp_location gs://machine-learning-dev-ttl-30d/anchorages/temp \
            --setup_file ./setup.py \
            --runner DataflowRunner \
            --disk_size_gb 100 \
            --region us-central1



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
