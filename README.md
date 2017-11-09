# Anchorages

## Creating Anchorage Points

Run:
  
    python -m anchorages --name updateanchorages \
                         --start-date YYYY-MM-DD \
                         --end-date YYYY-MM-DD \
                         --output BQ_TABLE_NAME \
                         --input-table INPUT_TABLE \
                         --fishing-mmsi-list FISHING_LIST


Standard dataflow options can also be specified.

For example, to run all years:

    python -m anchorages --name anchoragesallyears \
                         --start-date 2012-01-01 \
                         --end-date 2017-12-31 \
                         --input-table pipeline_classify_p_p516_daily \
                         --max_num_workers 200 \
                         --fishing-mmsi-list ../../treniformis/treniformis/_assets/GFW/FISHING_MMSI/KNOWN_LIKELY_AND_SUSPECTED/ANY_YEAR.txt

Or to run a minimal testing run:

    python -m anchorages --name testanchorages2016tiny \
                         --start-date 2016-01-01 \
                         --end-date 2016-01-31 \
                         --input-table pipeline_classify_p_p516_daily \
                         --fishing-mmsi-list ../../treniformis/treniformis/_assets/GFW/FISHING_MMSI/KNOWN_LIKELY_AND_SUSPECTED/ANY_YEAR.txt


## Naming Anchorage Points

After a set of anchorages is created, names are assigned using `name_anchorages_main`

For example:

    python -m name_anchorages_main \
                  --name testnameanchorages \
                  --input-table machine_learning_dev_ttl_30d.anchorages_anchoragesallyears \
                  --output-table machine_learning_dev_ttl_30d.test_anchorage_naming \
                  --override-path ./anchorage_overrides.csv

The override path points to a csv file containing anchorages that are either missing or incorrectly named.
It should have the following fields: s2uid,label,iso3,anchor_lat,anchor_lon,sublabel.


## Updating Port Events

To update a single day of events, run:

    python -m port_events --name JOB_NAME \
                          --anchorage-path GS_PATH_TO_ANCHORAGES \
                          --start-date YYYY-MM-DD \
                          --end-date YYYY-MM-DD 
                          --output BQ_TABLE_NAME

For example:

    python -m port_events --name portvisitsoneday \
                          --anchorages gfw_raw.anchorage_naming_20171026 \
                          --start-date 2016-01-01 \
                          --end-date 2016-01-01 \
                          --max_num_workers 100

Results are **appended** to the specified file.

For a full list of options run:

    python -m port_visits_main -h


## Config file

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
