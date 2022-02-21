# RAINBOW-Distributed data storage

Rainbow's distributed data storage is developed using the [**Apache Ignite**](https://ignite.apache.org/) main-memory database.

The storage component comprises of 2 instances, the simple server node and the cluster head.

## Server

The Server is responsible for storing local data and/or remote data depending on other nodes' condition. 

It incorporates the services:

- Http Service: which is the API for ingesting and extracting data for RAINBOW services.
- Data movement service: which is responsible for transferring ingestion and extraction requests of monitoring data. It is also responsible for replicating local data to remote nodes if the cluster head requests it.
- Data management service: which is a low-level service responsible for directly writing/reading data to the different local caches.

### Http service

The Http Service is responsible for handling requests from external services and provides the following endpoints:

- `/nodes`: `POST` request that returns the list of active storage instances with their hostnames and their type of instance.
- `/put`: `POST` request for ingestion of monitoring data.
- `/get`: `POST` request that returns monitoring data with their values.
- `/query`: `POST` request that returns an aggregated value from the monitoring data. 
- `/list`: `POST` request that returns a list of the monitoring metadata. 
- `/monitoring`: `DELETE` request that deletes the specified monitoring data.
- `/analytics/put`: `POST` request for ingestion of analytics data.
- `/analytics/get`: `POST` request that returns analytics data with their values.
- `/analytics`: `DELETE` request that deletes the specified analytics data.
- `/app/put`: `POST` request for ingestion of app data.
- `/app/get`: `POST` request that returns app data with their values.
- `/app`: `DELETE` request that deletes the specified app data.

## Cluster head

The cluster head instance is responsible for monitoring the Ignite cluster and moving monitoring data when deemed necessary. It is also used to store and extract data as a simple server node. 

It incorporates all the services of the simple server node with the addition of 1 service: 

- Movement service: which is responsible for monitoring the restarts/fails of the Ignite cluster and deciding whether a node should start replicated its data points to a remote one. The process is automated and runs on specific time intervals. 

## Caches

Ignite uses caches for both persistent and in-memory data. Each server node along with the cluster head have 4 local caches for storage of both persistent and in-memory data.

- *LatestMonitoring* cache is used for in-memory storage of the latest values for every monitoring metric.
- *HistoricalMonitoring* cache is used for persistent storage of the historical values for every monitoring metric.
- *Analytics* cache is used for persistent storage of processed data from the Analytics component.
- *ApplicationData* cache is used for in-memory storage of simple key-value data with timestamps.

Along with the local caches, each node has a copy of the 3 replicated caches that are used from the placement and the movement service.

- *MetaMonitoring* cache is used for persistent storage of metadata for every monitoring metric and the entity they belong to.
- *Replicas* cache is used to store the remote nodes that a monitoring data point is replicated.
- *Restarts* cache is used to store the restarts/fails of the server nodes in the Ignite cluster.

### Data Regions

2 data regions are available with specified off-heap memory limits. 

- *Persistent_region*: is used for the persistent caches with default max off-heap memory size to 256MB.
- *Default_region*: is the region for every in-memory cache with default max off-heap memory size to 256MB.

The default total size of the regions is 512MB split evenly between them (256MB for persistent region and 256MB for in-memory). The user can optionally change the total memory size with the **SIZE** environment variable. *The minimum off-heap size for each region is 100MB.*

## Deployment

In an **actual deployment** a single cluster head instance needs to be deployed first in order to create the cluster and the necessary caches. If many instances are deployed concurrently some of them may not be able to enter the cluster since each of them tries to create one.

The docker image can be created using the `build_image.sh` script in the respective folder.

## Configuration

Environment variables control the optional features such as persistence and the instance type (server, client). A list of all available variables that can be used in the docker container is below:

1. **CLUSTER_HEAD**: The variable that controls the instance type. Available values are "**true**" and "**false**". *Default value is "**false**"*.
2. **HOSTNAME**: The variable that is used for the container's hostname. If it is skipped, the program tries to find its own hostname using the `InetAddress` library.
3. **DISCOVERY**: The variable that controls the discovery process. It should be a comma separated list of hostnames, e.g "**server-1,server-2**".
4. **APP_CACHE**: The variable that controls whether the user-application cache is on or off. Available values are "**true**" and "**false**". *Default value is "**false**"*.
5. **EVICTION**: The variable that controls the eviction period for in-memory data. The value represents the eviction rate in hours from the creation of a data row. *Default value is "**168** hours (1 week)"*.
6. **SIZE**: The variable that controls the total off-heap size for the data regions. Each region gets half of the total size as the maximum off-heap memory size.
7. **JAVA_OPTS**: The variable that controls different JVM options. The user can change the max JVM heap memory size (e.g. "-Xms256m -Xmx256m").
8. **PLACEMENT**: The variable that controls whether the placement service will be on or off. Available values are "**true**" and "**false**". *Default value is "**true**"*.

## Docker ports

The available ports that are exposed from the Docker deployment through the above process are:

- 50000: REST API for the Server instance.
- 47500: Used for the discovery of the cluster.
- 47100: Used for the communication of the instances.

## REST API examples

* For the monitoring data, a `/put` **POST request** needs to have a similar body:

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
        "timestamp": 1611318068003,
        "pod":{
            "uuid": "123",
            "name": "kube",
            "namespace": "namespace"
        },
        "container":{
            "id": "345",
            "name": "container"
        }
    }
]}
```

The `entityID`, `metricID`, `val` and `timestamp` fields are mandatory.

* For the monitoring data, a `/get` **POST request** needs to have a similar body:

```
{   
    "metricID": ["metr1"], (optional)
    "entityID": ["ent1"], (optional)
    "podName": ["pod1"], (optional)
    "podNamespace": ["name1"], (optional)
    "containerName": ["container1"], (optional)
    "from": 1611318068000, (optional)
    "to": 2611318068000, (optional)
    "nodes": ["ip1","ip2"] (optional)
}
```
The `from` and `to` variables are optional. If they are missing only the latest value will be returned. The filters `metricID`, `entityID`, `podName`, `podNamespace` and `containerName` can be skipped or present in any combination for filtering out the results.

Using the optional `nodes` keyword with an array of ips (or empty array) returns the requested metric from a set of server nodes (or every active server node):

An example **response** body is:

```
{
    "monitoring": [
        {
            "node": "ip1",
            "data": [
                {
                    "metricID": "metr1",
                    "entityID": "ent1",
                    "values": [
                        {
                            "timestamp": 3,
                            "val": 30.0
                        }
                    ],
                    "entityType": "fog",
                    "name": "myname",
                    "units": "myunits",
                    "desc": "this is great",
                    "group": "mygroup",
                    "minVal": 5.0,
                    "maxVal": 10.0,
                    "higherIsBetter": true,
                    "pod": {
                        "uuid": "123",
                        "name": "kube",
                        "namespace": "namespace"
                    },
                    "container": {
                        "id": "345",
                        "name": "container"
                    }
                },
                {...},
                {...}
            ]
        },
        {...}
    ]
}
```

* For the aggregated query, a `\query` **POST request** needs to have the same body with the `\get` request in addition to one of the aggregated functions:

```
"agg": "sum"/"max"/"min"/"avg"

```

An example **response** body is:

```
{
    "value": 30.0
}
```

* For the list of monitoring data, a `/list` **POST request** needs to have a similar body:

```
{
    "metricID": ["metr1"], (optional)
    "entityID": ["ent1"], (optional)
    "podName": ["pod1"], (optional)
    "podNamespace": ["name1"], (optional)
    "containerName": ["container1"], (optional)
    "nodes": ["ip1"], (optional)
    "fields": ["metricfda", "entityType", "containerName", "podName", "desc", "podUUID", "higherIsBetter"] (optional)
}
```

The filters and the node list are used in the same way as the `\get` request for monitoring data. The *fields* keyword is used to reduce the metadata that are returned.

An example **response** body is:

```
{
    "metric": [
        {
            "node": "ip1",
            "metricID": "metr1",
            "entityID": "as1",
            "entityType": "fog",
            "higherIsBetter": true,
            "desc": "this is great",
            "pod": {
                "podName": "kube",
                "podUUID": "123"
            },
            "container": {
                "containerName": "container"
            }
        }
    ]
}
```

* For the analytics data, a `/analytics/put` **POST request** needs to have a similar body:

```
{"analytics": [
    {   
        "key": "app1",
        "val": "app1data",
        "timestamp": 1611318068000
    }
]}
```

All 3 fields are mandatory.

* For the analytics data, a `/analytics/get` **POST request** needs to have a similar body:

```
{   
    "key": ["key1"], (optional)
    "from": 1611318068000, (optional)
    "to": 2611318068000 (optional)
    
}
```

An example **response** body is:

```
{
    "analytics": [
        {
            "key": "key1",
            "values": [
                {
                    "timestamp": 1611318068000,
                    "val": 3.0
                }
            ]
        }
    ]
}
```

* The application-specific endpoints work the **same as the analytics ones**.