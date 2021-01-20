# RAINBOW-Distributed data storage

Rainbow's distributed data storage is developed using the [**Apache Kafka**](https://ignite.apache.org/) main-memory database.

The storage component comprises of 2 different instances, the ignite-server and the ignite-client ones.

##Ignite-server

Ignite-server is responsible for storing local data and/or remote data depending on other nodes' resource congestion. 

It incorporates 3 different services:

- Ingestion service: which is responsible for ingesting data via a socket connection.
- Extraction service: which is responsible for extracting locally stored data via a socket connection.
- Rebalance service: which is responsible for rebalancing the ingested data based on the node's resource congestion. This service is called internally after every ingestion task is completed to check if data replication/partitioning is needed. 

Rebalancing/partitioning strategy currently checks only the CPU load of the local node and chooses a random remote server instance if rebalancing is needed.

## Ignite-client

Ignite-client is responsible for extracting data from all/some remote nodes concurrently. 

It incorporates 1 service: 

- Extraction service: which is responsible for extracting the queried data from the necessary server instances via socket connection. It can either be used to extract the metadata that store the information about the rebalanced source/target instances or to extract stored data from all necessary server instances in a single query. 

## Deployment

The `docker-swarm.yml` file can be used to deploy 2 instances of `ignite-server` and 1 instance of `ignite-client` nodes on available swarm nodes. It also deploys [**Apache Zookeeper**](https://zookeeper.apache.org/) instances needed to store metadata on the cluster nodes.

Each instance image can be created using the `build_image.sh` script in the respective folder.

## Docker ports

The available ports that are exposed from the Docker deployment through the above process are:

- 50000: Ingestion service for the Ignite-server instance
- 50001: Extraction service for the Ignite-server instance 
- 50002: Extraction service for the Ignite-client instance
