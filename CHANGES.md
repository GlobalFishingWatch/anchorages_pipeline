# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
