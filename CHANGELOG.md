# Changelog

## [1.4.3] - 28-02-2022

### Changed

- Fixes based on sonarqube
- Fix on instance name

## [1.4.0] - 21-02-2022

### Added

- New endpoints for deleting data (monitoring, analytics, app).
- New endpoint for getting the list of monitoring metadata.
- New endpoint for getting list of active nodes
- Aggregation queries (and endpoint) on monitoring data.
- New service in charge of data movement for monitoring data. I.e. getting data from remote nodes and replicating local data to remote ones. 
- New service in charge of data placement based on node restarts/fails (can be optionally turned off using an environment variable).
- Two new replicated caches that help store metadata for the placement and movement services.
- New environment variable `CLUSTER_HEAD` which creates the main server node that initialize the caches and runs the placement service.

### Changed

- Monitoring metadata cache is now replicated to all nodes.
- Changed analytics cache to be able to store historical data.
- Split http service from the data management and movement services.
- Updated unit tests
- Refactoring of the codebase with SonarLint recommendations.

### Removed

- Client option is removed since the cluster head option does the same job and stores analytics data.

## [1.3.5] - 23-11-2021

### Added

- Wildcards on filters for get queries.

## [1.3.4] - 09-11-2021

### Added

- New fields in the metadata of the monitoring data about the pod and container info.
- Added the unique combination of `metricID` and `entityID` as key to every stored cache of monitoring data.
- New filters on `podName`, `podNamespace` and `containerName` based on the new metadata on the `get` request.

### Changed

- Changed the `metricID` and `entityID` filters on the `get` request for the monitoring data to be used alongside the new ones in any combination.

### Removed

- The `latest` field from the `get` request on the monitoring data. If no `from` and `to` fields are present then the latest data are returned.

## [1.3.3] - 18-10-2021

### Added

- JVM options on dockerfiles for GC and heap memory. Optional user input for heap memory throught the JAVA_OPTS environment variable.
- Added option for monitoring and analytics `get` commands to return data from a set (or all) server nodes based on their hostnames/ips.

### Changed

- Optional user input for regions total size. Default total size is 512MB which is split evenly between the in-memory and persistent regions.

## [1.3.2] - 11-10-2021

### Added

- Maximum size (500MB) for default data region (for latest and analytics data).
- Maximum size (1GB) for persistent data region (for historical and meta data).
- New region with maximum size of 200MB for application data.
- Gitlab CI/CD pipeline

### Changed

- Persistent is no longer optional. Historical and Metadata caches are persistent-only.
- Dockerfile for gitlab CI/CD pipeline

## [1.3.1] - 01-06-2021

### Added 

- In-memory cache for processed analytic results using a key-value model.
- REST API routes for ingesting and extracting analytics data.
- New optional environment variable for the container's hostname.
 
## [1.3.0] - 22-05-2021

### Added 

- Client REST API to extract monitoring data from all ignite server nodes.
- In-memory key-value cache for user application data.
- Persistence options for historical and metadata caches.
- Eviction options for historical and metadata caches.
- Unit tests for the main functions of the Server instances (ingest, extract monitoring data).

### Changed 

- Merged the docker images (client and server) into one image.
- The docker image starts with a base of JDK 1.8 and Alpine instead of Ignite.
- Ignite is initialized through the code main function.
- Server or Client nodes are decided through an environment variable.
- Configuration of instances is implemented inside the code instead of XML files.
- Changed the `DataService` interface and implementation to incorporate 2 public classes that can be used from both server and client APIs.

## [1.2.2] - 04-02-2021 (bugfix)

### Changed 

- Fixed bug on sql queries with WHERE clause.
- Changed zookeeper service name.

## [1.2.1] - 29-01-2021 (hotfix)

### Changed 

- Changed `get` filter to get all metrics when there is no metricID or entityID keyword.

## [1.2.0] - 29-01-2021

### Added 

- Caches for storing historical data and metadata for all monitoring metrics.
- New filters for the `get` endpoint of `Ignite-server` in order to filter out returned values based on metricID/entityID and time period or latest values.
- `docker-compose.yml` file to deploy the solution with a single Ignite service and a zookeeper.

## [1.1.0] - 22-01-2021

### Added 

- Class for metrics with metadata.

### Changed

- Ingestion and Extraction services of `Ignite-server` are incorporated into a single REST API instead of a simple socket connection.
- Ingestion service does not call rebalancing service on each ingest task.
- Stopped `Ignite-client`'s extraction service in order to incorporate it in a REST API.  

## [1.0.1] - 20-01-2021

### Added

- Try-catch blocks incorporated on all input/output tasks for the `Ignite-server` services.
- Added conditions to check if there are available nodes for rebalancing and if the external node is still available on every rebalancing task.
- Added README and CHANGELOG files.

### Changed

- Fixed `build_image.sh` script to automatically get the project's version from pom and create the image with this version.

## [1.0.0] - 01-12-2020

### Added

- Initial working version with starting services and instances.
- 2 instances of Ignite, `Ignite-server` and `Ignite-client`.
- 3 services for `Ignite-server` (Ingestion, Extraction, Rebalance).
- 1 service for `Ignite-client` (Extraction).

[1.4.3]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.4.3
[1.4.2]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.4.2
[1.4.1]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.4.1
[1.4.0]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.4.0
[1.3.5]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.3.5
[1.3.4]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.3.4
[1.3.3]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.3.3
[1.3.2]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.3.2
[1.3.1]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.3.1
[1.3.0]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.3.0
[1.2.2]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.2.2
[1.2.1]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.2.1
[1.2.0]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.2.0
[1.1.0]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.1.0
[1.0.1]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.0.1
[1.0.0]: https://gitlab.com/rainbow-project1/rainbow-storage/-/tree/v.1.0.0