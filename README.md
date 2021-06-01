# RAINBOW-Distributed data storage

Rainbow's distributed data storage is developed using the [**Apache Ignite**](https://ignite.apache.org/) main-memory database.

The storage component comprises of 2 different instances, the server and the client ones.

## Server

The Server is responsible for storing local data and/or remote data depending on other nodes' resource congestion. 

It incorporates the services:

- Monitoring Ingestion service: which is responsible for ingesting monitoring data via REST API with `/put` route. The API expects a POST request with a json containing the metrics.
- Monitoring Extraction service: which is responsible for extracting locally stored monitoring data via REST API with `/get` route. The API expects a POST request either empty or with a json containing the filter.
- Application Ingestion service: which is responsible for ingesting key-value pairs via REST API with `/app/put` route. The API expects a POST request with a json containing the keys and values.
- Application Extraction service: which is responsible for extracting locally stored key-value pairs via REST API with `/app/get` route. The API expects a POST request either empty or with a json containing the filter.
- (*IN PROGRESS*) Rebalance service: which is responsible for rebalancing the ingested data based on the node's resource congestion. This service is called internally after every ingestion task is completed to check if data replication/partitioning is needed.  

Rebalancing/partitioning strategy currently checks only the CPU load of the local node and chooses a random remote server instance if rebalancing is needed (*not operational*).

## Client

Client is responsible for extracting data from all/some remote nodes concurrently. 

It incorporates 1 service: 

- Extraction service: which is responsible for extracting the queried data from every server instance via REST API with `/get` route. 

## Caches

Ignite uses caches for both persistent and in-memory data. Ignite-server uses 3 caches for storage of both persistent and in-memory data. Ignite-client uses 1 cache for metadata of the Ignite-server instances.

- *LatestMonitoring* cache is used for in-memory storage of the latest values for every monitoring metric.
- *HistoricalMonitoring* cache is used for persistent (currently in-memory) storage of the historical values for every monitoring metric.
- *MetaMonitoring* cache is used for persistent (currently in-memory) storage of metadata for every monitorinc metric and the entity they belong to.

For the user application data 1 in-memory key-value cache is used, namely **ApplicationData**.

Persistence and eviction rate are optional. If persistence is enabled the cluster takes longer to initiate each new node and insert it into the baseline topology (auto-adjustment is on).

## Deployment

In an **actual deployment** a single instance needs to be deployed first in order to create the cluster. If many instances are deployed concurrently some of them may not be able to enter the cluster since each of them tries to create one.

The `docker-swarm.yml` file can be used to deploy 1 instance of `ignite-server` and 1 instance of `ignite-client` nodes on available swarm nodes. Afterwards the service can be scaled up for every available node. For node discovery the IP discovery protocol is used. 

The docker image can be created using the `build_image.sh` script in the respective folder.

## Configuration

Environment variables control the optional features such as persistence and the instance type (server, client). A list of all available variables that can be used in the docker container is below:

1. **NODE**: The variable that controls the instance type. Available values are "**SERVER**" and "**CLIENT**". *Default value is "**SERVER**"*.
2. **HOSTNAME**: The variable that is used for the container's hostname. If it is skipped, the program tries to find its own hostname using the `InetAddress` library.
2. **DISCOVERY**: The variable that controls the discovery process. It should be a comma separated list of hostnames, e.g "**server-1,server-2**".
3. **PERSISTENCE**: The variable that controls whether persistence is on or off. Available values are "**true**" and "**false**". *Default value is "**false**"*.
4. **APP_CACHE**: The variable that controls whether the user-application cache is on or off. Available values are "**true**" and "**false**". *Default value is "**false**"*.
5. **EVICTION**: The variable that controls the eviction period when persistence is on. The value represents the eviction rate in hours from the creation of a data row. *Default value is "**168** hours (1 week)"*.

## Docker ports

The available ports that are exposed from the Docker deployment through the above process are:

- 50000: REST API for the Server instance
- 50001: Extraction service for the Client instance

## REST API examples

* For the monitoring data, a `/put` POST request needs to have a similar body:

```
{"monitoring": [
    {   
        "entityID": "ent1",
        "entityType": "fog",
        "metricID": "metr1",
        "name": "cpu",
        "units": "units1",
        "desc": "CPU",
        "group": "group1",
        "minVal": 5,
        "maxVal": 10,
        "higherIsBetter": true,
        "val": 6,
        "timestamp": 1611318068003
    }
]}
```

* For the monitoring data, a `/get` POST request needs to have a similar body:

```
{   
    "metricID": ["metr1"],
    "from": 1611318068000, 
    "to": 2611318068000, 
    "latest": false
}
```
The `from` and `to` variables are optional if the `latest` variable is true. The `metricID` can also be `entityID` to get all metrics from an entity or skipped entirely to get all available metrics.

* For the user-application data, a `/app/put` POST request needs to have a similar body:

```
{"application": [
    {   
        "key": "app1",
        "value": "app1data"
    }
]}
```

* For the user-application data, a `/app/get` POST request needs to have a similar body:

```
{   
    "key": ["app1"]
}
```