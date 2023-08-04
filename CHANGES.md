# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## v3.4.0 - 2023-08-02

### Added

* [PIPELINE-1319](https://globalfishingwatch.atlassian.net/browse/PIPELINE-1319): Adds
  support for runnig the code with `python-3.8` and updated the google sdk from
  `2.40.0` to [2.49.0](https://github.com/apache/beam/releases/tag/v2.49.0).

## v3.3.2 - 2023-07-18

### Added

Adds files via upload. Anchorage overrides including 2022 changes Nate Miller and CianLuck provided in label review

### Changed

* [ENG-521](https://globalfishingwatch.atlassian.net/browse/ENG-521): Changes
  Updates Google SDK version to avoid vulnerabilities.

## v3.3.1 - 2022-07-21

### Changed

* [PIPELINE-914](https://globalfishingwatch.atlassian.net/browse/PIPELINE-914): Changes
  version of `Apache Beam` from `2.35.0` to [2.40.0](https://beam.apache.org/blog/beam-2.40.0/).

## v3.3.0 - 2022-03-08

### Added

* [PIPELINE-808](https://globalfishingwatch.atlassian.net/browse/PIPELINE-808): Adds
  Dockerfile for scheduler and worker, to diferentiate the requirements in both cases.
  Prepared the cloudbuild.

## v3.2.4 - 2021-09-16

### Added

* [PIPELINE-536](https://globalfishingwatch.atlassian.net/browse/PIPELINE-536): Adds
  Wait for Dataflow job option parameter in port_events and port_visits, to sync when the job is completed.

## v3.2.1 - 2021-06-14

### Changed

* [PIPELINE-439](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/439): Changes
  Automte the compatibility version of port visits and voyages.
  the new compatibility automate consists in outputting the following tables
  - proto_raw_port_events_YYYYMMDD
  - proto_port_state
  - proto_port_visists
  - proto_voyages_c2
  - proto_voyages_c3
  - proto_voyages_c4
  - port_visits_YYYYMMDD (Compatibility part)
  - voyages (Compatibility part)

* [PIPELINE-399](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/399): Changes
  Automate the new port schema in Airflow.
  Port-visits and voyages are in same DAG. Port visits runs across all the
  encounters and completely regenerate the table every day. The port-visit
  table tooks about 270GB, and is deleted when the re-generate process starts.

## v3.2.0 - 2021-05-11

* [PIPELINE-380](https://github.com/GlobalFishingWatch/anchorages_pipeline/pull/380)
  - Switch from vessel-id to segment-id for port events. This makes the the port events table a 
    static table and opens up the possibility of filtering by bad segments later.
  - Rather than using a finite lookback, we create an auxilliary table keeping track of the current 
    state so that we can look back "infinitely"
  - Continue to use vessel-id for port-visits, which requires a table mapping seg-id to vessel-id, 
    This keeps the output similar to previously but makes the separation between static tables (events)
    and dynamic tables (visits) explicit.
  - Support a bad-segments table that filters events before they are assembled into visits.

## v3.1.0 - 2021-05-03

### Added

* [PIPELINE-84](https://globalfishingwatch.atlassian.net/browse/PIPELINE-84): Adds
  support for Apache Beam `2.28.0`.
  Increments pipe-tools to version `v3.2.0`.
  Increments Google SDK version to `338.0.0`.
  Fixes timestamp format parsing in records.

## v3.0.4 - 2020-12-01

### Fixed

* [PIPELINE-256](https://globalfishingwatch.atlassian.net/browse/PIPELINE-256):
  Fix port visits wrongly setting `start_lon` to the same value it's using for
  `start_lat`.

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
