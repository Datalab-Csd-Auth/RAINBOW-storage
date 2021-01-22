# Changelog

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

[1.1.0]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.1.0
[1.0.1]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.0.1
[1.0.0]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.0.0