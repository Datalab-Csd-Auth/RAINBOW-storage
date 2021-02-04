# Changelog

## [1.2.2] - 04-02-2021 (bugfix)

### Changed 

- Fixed bug on sql queries with WHERE clause
- Changed zookeeper service name

## [1.2.1] - 29-01-2021 (hotfix)

### Changed 

- Changed `get` filter to get all metrics when there is no metricID or entityID keyword

## [1.2.0] - 29-01-2021

### Added 

- Caches for storing historical data and metadata for all monitoring metrics
- New filters for the `get` endpoint of `Ignite-server` in order to filter out returned values based on metricID/entityID and time period or latest values.
- `docker-compose.yml` file to deploy the solution with a single Ignite service and a zookeeper

## [1.1.0] - 22-01-2021

### Added 

- Class for metrics with metadata

### Changed

- Ingestion and Extraction services of `Ignite-server` are incorporated into a single REST API instead of a simple socket connection.
- Ingestion service does not call rebalancing service on each ingest task.
- Stopped `Ignite-client`'s extraction service in order to incorporate it in a REST API.  

## [1.0.1] - 20-01-2021

### Added

- Try-catch blocks incorporated on all input/output tasks for the `Ignite-server` services
- Added conditions to check if there are available nodes for rebalancing and if the external node is still available on every rebalancing task
- Added README and CHANGELOG files

### Changed

- Fixed `build_image.sh` script to automatically get the project's version from pom and create the image with this version

## [1.0.0] - 01-12-2020

### Added

- Initial working version with starting services and instances
- 2 instances of Ignite, `Ignite-server` and `Ignite-client` 
- 3 services for `Ignite-server` (Ingestion, Extraction, Rebalance)
- 1 service for `Ignite-client` (Extraction)

[1.2.1]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.2.1
[1.2.0]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.2.0
[1.1.0]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.1.0
[1.0.1]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.0.1
[1.0.0]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.0.0