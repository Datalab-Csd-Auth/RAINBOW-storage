# RAINBOW-Distributed data storage

Rainbow's distributed data storage is developed using the [**Apache Ignite**](https://ignite.apache.org/) main-memory database.

The storage component comprises of 2 different instances, the ignite-server and the ignite-client ones.

## Ignite-server

Ignite-server is responsible for storing local data and/or remote data depending on other nodes' resource congestion. 

It incorporates 3 different services:

- Ingestion service: which is responsible for ingesting data via REST API with `put` route. The API expects a POST request with a json containing the metrics.
- Extraction service: which is responsible for extracting locally stored data via REST API with `get` route. The API expects a POST request either empty or with a json containing the filter.
- (*IN PROGRESS*) Rebalance service: which is responsible for rebalancing the ingested data based on the node's resource congestion. This service is called internally after every ingestion task is completed to check if data replication/partitioning is needed.  

Rebalancing/partitioning strategy currently checks only the CPU load of the local node and chooses a random remote server instance if rebalancing is needed.

## Caches

Ignite uses caches for both persistent and in-memory data. Ignite-server uses 3 caches for storage of both persistent and in-memory data. Ignite-client uses 1 cache for metadata of the Ignite-server instances.

- *LatestMonitoring* cache is used for in-memory storage of the latest values for every monitoring metric.
- *HistoricalMonitoring* cache is used for persistent (currently in-memory) storage of the historical values for every monitoring metric.
- *MetaMonitoring* cache is used for persistent (currently in-memory) storage of metadata for every monitorinc metric and the entity they belong to.

Persistence and eviction rate are implemented. **BUT** if persistence is enabled the cluster needs to be activated through a shell command or API call from one of the nodes.

## Ignite-client

Ignite-client is responsible for extracting data from all/some remote nodes concurrently. 

It incorporates 1 service: 

- (*IN PROGRESS*) Extraction service: which is responsible for extracting the queried data from the necessary server instances via socket connection. It can either be used to extract the metadata that store the information about the rebalanced source/target instances or to extract stored data from all necessary server instances in a single query. 

## Deployment

In an **actual deployment** a single instance needs to be deployed first in order to create the cluster. If many instances are deployed concurrently some of them may not be able to enter the cluster since each of them tries to create one.

If **persistence is enabled**, after the first instance is deployed and the cluster is created a job needs to run to activate the cluster using the command `./apache-ignite/bin/control.sh --set-state active --yes` from inside the ignite instance. This initializes the cluster for every other/new instances. If the state is not changed to **active** then the cluster will not work.

The `docker-swarm.yml` file can be used to deploy 2 instances of `ignite-server` and 1 instance of `ignite-client` nodes on available swarm nodes. It also deploys [**Apache Zookeeper**](https://zookeeper.apache.org/) instances needed to store metadata on the cluster nodes.

Each instance image can be created using the `build_image.sh` script in the respective folder.

## Docker ports

The available ports that are exposed from the Docker deployment through the above process are:

- 50000: REST API for the Ignite-server instance
- 50002: Extraction service for the Ignite-client instance
