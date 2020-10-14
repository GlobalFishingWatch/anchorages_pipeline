# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]


## v3.0.4 - 2020-10-14

### Added

* [Data Pipeline/PIPELINE#31](https://globalfishingwatch.atlassian.net/browse/PIPELINE-31): Adds
  * Creation of frozen dependencies in the docker image at building time.
  * Replaces requirements.txt to frozen dependencies for dataflow operators.
  * Google SDK increment version to `314.0.0`.
  * description in `README.md`.

## v3.0.3 - 2020-06-11

### Added

* [GlobalFishingWatch/gfw-eng-tasks#111](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/111): Adds
  * Pin to `pipe-tools:v3.1.2`.

## v3.0.2 - 2020-03-18

### Changed

* [GlobalFishingWatch/gfw-eng-tasks#45](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/45): Changes
    on print to avoid failing task.

## v3.0.1 - 2020-03-18

### Changed

* [GlobalFishingWatch/gfw-eng-tasks#28](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/28): Changes
    Extract `messages_segmented_table` and `segments_table` as parameters from `pipe_anchorages/anchorages_pipeline.py`. 
    Adds `messages_segmented_table` and `segments_table` as required parameters in `pipe_anchorages/options/anchorage_options.py`.
    Changes the way the dags are instantiating.
    Updates the `README.md`.
    Fixes the tests.

## v3.0.0 - 2020-03-09

### Added

* [GlobalFishingWatch/gfw-eng-tasks#28](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/28): Adds
    support `pipe-tools:v3.1.1`.
    updates the cloud sdk version.

## v1.3.1 - 2019-07-30

### Added

* [GlobalFishingWatch/GFW-tasks#49](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/49)
 * Add dummy port visits at a far future time, so that voyages that are currently underway show up in the voyages table. Similarly, add dummy port visits in the
far past so that voyages that were underway when they first show up in AIS show up in the table.
* [GlobalFishingWatch/GFW-Tasks#1108](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1108) 
 * Add vessel-id to the voyages query

## v1.2.0 - 2019-05-23

### Added

* [GlobalFishingWatch/GFW-Tasks#1034][https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1034]
  * Added DAG for generating the voyages on a daily.

## v1.1.0 - 2019-05-23

### Added

* [#44][https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/44]
  * Add port gap events and only emit port visits if there is a port visit
    or port gap.

## v1.0.0 - 2019-03-27

### Added

* [GlobalFishingWatch/GFW-Tasks#991](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/991)
   * Migrates to use the new [airflow-gfw](https://github.com/GlobalFishingWatch/airflow-gfw) library and use the pipe-tools [v2.0.0](https://github.com/GlobalFishingWatch/pipe-tools/releases/tag/v2.0.0)

## 0.3.3 - 2019-03-13

* [GlobalFishingWatch/GFW-Tasks#987](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/987)
  * Add yearly mode dags.
* Add parameters for dataflow max_workers and disk_size_gb  
* Modify the default parameter in post_install to use the latest named table.

## 0.3.1 - 2018-11-15

* [#39](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/39)
  * Ensure the creation of empty tables when the dataflow process does not generate content.
  * Updates the operator call to support interval dates.

## 0.3.0 - 2018-09-07

* [#37](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/37)
  * Removes the publication of events from this pipeline, which will be handled on [pipe-events](https://github.com/globalfishingwatch/pipe-events). See [pipe-events#7](https://github.com/GlobalFishingWatch/pipe-events/pull/7).

## 0.2.1 2018-09-03

* [#36](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/36)
  * Bump version of pipe-tools to 0.1.7

## 0.2.0  2018-05-14

* [#31](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/31)
  * Use Vessel ID for Port Events and Port Visits
* [#32](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/32)
  * Publish standardized port in/out events
  * Update to pipe-tools v0.1.6  

## 0.1.23

* [#26](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/26)
  * Re-arrange airflow dags to make them importable into pipe_reference

## 0.1.22

* [#19](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/19)
  * Update Dag parameters to conform to new pipeline (copied form pipe-segment).

## 0.1.0

* [#9](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/9)
  * Reorganize port lists so that it is easier to add new lists.
  * Add `label_source` field.
  * Reduce job file size.

## 0.0.4

* [#7](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/7)
  * Update name override file for Curacao anchorages

## 0.0.3

* [#6](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/6)
  * Update name override file following top anchorages review.

## 0.0.2

* [#5](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/5)
  * Normalize names using unidecode and upper.

* [#4](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/4)
  * Dockerize so code can be run through docker.

## 0.0.1

* [#3](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/3)
  * Setup framework for tracking versions and changes.
