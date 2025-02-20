package org.auth.csd.datalab.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.Helpers;
import org.auth.csd.datalab.common.interfaces.DataManagementInterface;
import org.auth.csd.datalab.common.interfaces.MovementInterface;
import org.auth.csd.datalab.common.models.InputJson;
import org.auth.csd.datalab.common.models.Monitoring;
import org.auth.csd.datalab.common.models.keys.HostMetricKey;
import org.auth.csd.datalab.common.models.keys.MetricKey;
import org.auth.csd.datalab.common.models.values.MetaMetric;
import org.auth.csd.datalab.common.Helpers.Tuple2;
import org.auth.csd.datalab.common.models.values.TimedMetric;

import javax.cache.Cache;
import java.util.*;
import java.util.stream.Collectors;

import static org.auth.csd.datalab.ServerNodeStartup.*;
import static org.auth.csd.datalab.common.Helpers.combineTuples;
import static org.auth.csd.datalab.common.Helpers.getQueryValues;

public class MovementService implements MovementInterface {

    @IgniteInstanceResource
    private Ignite ignite;
    private static final String SQL_WHERE = " WHERE ";
    /**
     * Reference to the cache.
     */
    private static IgniteCache<HostMetricKey, MetaMetric> myMeta;
    private static IgniteCache<HostMetricKey, Set<String>> myReplica;
    private boolean replicate = false;

    private void metaTransform(HashMap<HostMetricKey, MetaMetric> result, QueryCursor<List<?>> cursor) {
        for (List<?> row : cursor) {
            HostMetricKey key = new HostMetricKey(row.get(0).toString(), row.get(1).toString(), row.get(15).toString());
            MetaMetric value = new MetaMetric(row.get(2).toString(), row.get(3).toString(), row.get(4).toString(), row.get(5).toString(),
                    row.get(6).toString(), (double) row.get(7), (double) row.get(8), (boolean) row.get(9),
                    row.get(10).toString(), row.get(11).toString(), row.get(12).toString(), row.get(13).toString(),
                    row.get(14).toString());
            result.put(key, value);
        }
    }

    //Extract the meta data for the monitoring data
    private HashMap<HostMetricKey, MetaMetric> extractMetaData(HashMap<String, HashSet<String>> filter, String... hostname) {
        String select = "SELECT metricID, entityID, entityType, name, units, desc, groupName, minVal, maxVal, higherIsBetter, podUUID, podName, podNamespace, containerID, containerName, hostname ";
        String from = "FROM METAMETRIC ";
        List<String> where = new ArrayList<>();
        HashMap<HostMetricKey, MetaMetric> result = new HashMap<>();
        if (!filter.isEmpty()) {
            for (Map.Entry<String, HashSet<String>> entry : filter.entrySet()) {
                List<String> tmpWhere = new ArrayList<>();
                List<String> wildcard = entry.getValue().stream().filter(el -> el.endsWith("%")).collect(Collectors.toList());
                List<String> exact = new ArrayList<>(entry.getValue());
                exact.removeAll(wildcard);
                if (!exact.isEmpty())
                    tmpWhere.add(entry.getKey() + " IN ('" + String.join("','", entry.getValue()) + "') ");
                for (String key : wildcard) {
                    tmpWhere.add(entry.getKey() + " LIKE '" + key + "' ");
                }
                where.add("(" + String.join(") OR (", tmpWhere) + ")");
            }
        }
        String hostWhere = (hostname.length == 1) ? " (hostname = '" + hostname[0] + "') " : " (hostname IN ('" + String.join("', '", hostname) + "')) ";
        String finalWhere = (!where.isEmpty()) ? SQL_WHERE + hostWhere + " AND (" + String.join(") AND (", where) + ") " : SQL_WHERE + hostWhere;
        try (QueryCursor<List<?>> cursor = getQueryValues(myMeta, select + from + finalWhere)) {
            metaTransform(result, cursor);
        }
        return result;
    }

    //Replicate every new data point coming from the monitoring service
    private void replicateNewMonitoring(HashMap<MetricKey, InputJson> metrics) {
        HashMap<String, HashMap<MetricKey, InputJson>> replicas = new HashMap<>();
        for (Map.Entry<MetricKey, InputJson> key : metrics.entrySet()) {
            Set<String> remotes = myReplica.get(new HostMetricKey(key.getKey(), localNode));
            if (remotes != null) {
                remotes.forEach(k -> {
                    if (replicas.containsKey(k)) {
                        HashMap<MetricKey, InputJson> tmpMetric = replicas.get(k);
                        tmpMetric.put(key.getKey(), key.getValue());
                        replicas.put(k, tmpMetric);
                    } else {
                        HashMap<MetricKey, InputJson> tmpHash = new HashMap<>();
                        tmpHash.put(key.getKey(), key.getValue());
                        replicas.put(k, tmpHash);
                    }
                });
            }
        }
        replicas.forEach((k, v) -> {
            if (!ignite.cluster().forServers().forHost(k).nodes().isEmpty()) {
                DataManagementInterface tmpInterface = ignite.services(ignite.cluster().forServers().forHost(k)).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
                tmpInterface.ingestMonitoring(v, localNode);
            }
        });
    }

    //Get the set of remote nodes for the metrics in question
    private HashMap<String, HashSet<HostMetricKey>> getReplicaSet(Set<HostMetricKey> metrics) {
        //Store the final set of metrics per node
        HashMap<String, HashSet<HostMetricKey>> finalSet = new HashMap<>();
        //Tmp sets for storing
        HashMap<String, HashSet<HostMetricKey>> metricsPerNode = new HashMap<>();
        HashSet<String> notedNodes = new HashSet<>();
        for (HostMetricKey key : metrics) {
            //If the metric is local then continue without checking replicas
            if (key.hostname.equals(localNode)) {
                //Add the hostname to the list
                notedNodes.add(localNode);
                HashSet<HostMetricKey> tmp = metricsPerNode.getOrDefault(localNode, new HashSet<>());
                tmp.add(key);
                metricsPerNode.put(localNode, tmp);
                continue;
            }
            //Get replicas
            Set<String> tmpReplicas = myReplica.get(key);
            //If replicas contain the local node, get just that
            if (tmpReplicas != null && tmpReplicas.contains(localNode)) {
                //Add the hostname to the list
                notedNodes.add(localNode);
                HashSet<HostMetricKey> tmp = metricsPerNode.getOrDefault(localNode, new HashSet<>());
                tmp.add(key);
                metricsPerNode.put(localNode, tmp);
                continue;
            }
            //If there are no replicas then it is a special case and the host of the key needs to be included in the necessary nodes
            if (tmpReplicas == null) notedNodes.add(key.hostname);
            else { //In every other case store all replicas and host
                tmpReplicas.forEach(k -> {
                    HashSet<HostMetricKey> tmp = metricsPerNode.getOrDefault(k, new HashSet<>());
                    tmp.add(key);
                    metricsPerNode.put(k, tmp);
                });
            }
            HashSet<HostMetricKey> tmp = metricsPerNode.getOrDefault(key.hostname, new HashSet<>());
            tmp.add(key);
            metricsPerNode.put(key.hostname, tmp);
        }
        //If the localnode is part of the solution then immediately add it to the final set
        if (metricsPerNode.containsKey(localNode)) {
            finalSet.put(localNode, metricsPerNode.get(localNode));
            metricsPerNode.remove(localNode);
        }
        //Same with every noted node
        notedNodes.forEach(k -> {
            if (metricsPerNode.containsKey(k)) {
                //Add it to the final set
                finalSet.put(k, metricsPerNode.get(k));
                //Remove it from the tmp map
                metricsPerNode.remove(k);
                //Remove elements from the rest of the nodes
                metricsPerNode.forEach((key, v) -> v.removeAll(finalSet.get(k)));
            }
        });
        while (!metricsPerNode.isEmpty()) {
            //Find the entry with the max number of metrics
            Map.Entry<String, HashSet<HostMetricKey>> maxEntry = null;
            for (Map.Entry<String, HashSet<HostMetricKey>> entry : metricsPerNode.entrySet()) {
                if (maxEntry == null || entry.getValue().size() > maxEntry.getValue().size()) {
                    maxEntry = entry;
                }
            }
            if (maxEntry == null || maxEntry.getValue().isEmpty()) {
                break;
            }
            //Then remove its elements from every other node
            //Add it to the final set
            finalSet.put(maxEntry.getKey(), maxEntry.getValue());
            //Remove it from the tmp map
            metricsPerNode.remove(maxEntry.getKey());
            //Remove elements from the rest of the nodes
            Map.Entry<String, HashSet<HostMetricKey>> finalMaxEntry = maxEntry;
            metricsPerNode.forEach((key, v) -> v.removeAll(finalMaxEntry.getValue()));
        }
        return finalSet;
    }

    //Start new replication process and write historical data to remote node
    @Override
    public void startReplication(Set<HostMetricKey> metrics, String remote) {
        //Start by getting the local data from the beginning
        DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
        HashMap<HostMetricKey, List<TimedMetric>> values = srvInterface.extractMonitoring(metrics, 0L, -1L);
        //Call the remote node to write them
        if (ignite.cluster().forServers().forHost(remote).nodes().size() == 1) {
            DataManagementInterface remoteSrvInterface = ignite.services(ignite.cluster().forServers().forHost(remote)).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
            remoteSrvInterface.ingestHistoricalMonitoring(values);
        }
        replicate = true;
    }

    @Override
    public void ingestMonitoring(HashMap<MetricKey, InputJson> metrics) {
        //First check if they exist in meta cache
        for (Map.Entry<MetricKey, InputJson> entry : metrics.entrySet()) {
            myMeta.putIfAbsent(new HostMetricKey(entry.getKey(), localNode), new MetaMetric(entry.getValue()));
        }
        //First store them in local node
        DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
        srvInterface.ingestMonitoring(metrics, localNode);
        //Then for each remote node call ingestMonitoring(metrics, localNode) service if there is a need to replicate
        if (replicate) replicateNewMonitoring(metrics);
    }

    @Override
    public HashMap<String, HashMap<MetricKey, Monitoring>> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to, HashSet<String> nodeList) {
        HashMap<String, HashMap<MetricKey, Monitoring>> result = new HashMap<>();
        if (nodeList == null) {//If nodelist is null then return local
            //Get local meta
            HashMap<HostMetricKey, MetaMetric> localMeta = extractMetaData(filter, localNode);
            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
            HashMap<HostMetricKey, List<TimedMetric>> values = srvInterface.extractMonitoring(localMeta.keySet(), from, to);
            //Create tmp result
            HashMap<MetricKey, Monitoring> tmpRes = new HashMap<>();
            localMeta.forEach((k, v) -> tmpRes.put(k.metric, new Monitoring(v, values.getOrDefault(k, new ArrayList<>()))));
            result.put(localNode, tmpRes);
        } else {
            String[] nodeArray;
            if (nodeList.isEmpty()) { //Else if it is empty return for every server
                nodeArray = ignite.cluster().forServers().hostNames().toArray(new String[0]);
            } else { //Return specified nodes
                nodeArray = nodeList.toArray(new String[0]);
            }
            //Get the meta queried
            HashMap<HostMetricKey, MetaMetric> meta = extractMetaData(filter, nodeArray);
            HashMap<String, HashSet<HostMetricKey>> nodeSet = getReplicaSet(meta.keySet());
            for (Map.Entry<String, HashSet<HostMetricKey>> node : nodeSet.entrySet()) {
                if (!ignite.cluster().forServers().forHost(node.getKey()).nodes().isEmpty()) {
                    DataManagementInterface srvInterface = ignite.services(ignite.cluster().forServers().forHost(node.getKey())).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
                    HashMap<HostMetricKey, List<TimedMetric>> values = srvInterface.extractMonitoring(node.getValue(), from, to);
                    values.forEach((k, v) -> {
                        HashMap<MetricKey, Monitoring> tmp = result.getOrDefault(k.hostname, new HashMap<>());
                        tmp.put(k.metric, new Monitoring(meta.get(k), v));
                        result.put(k.hostname, tmp);
                    });
                }
            }
        }
        return result;
    }

    @Override
    public Double extractMonitoringQuery(HashMap<String, HashSet<String>> filter, Long from, Long to, HashSet<String> nodeList, int agg) {
        Double tmpVal = (agg == 1) ? Double.MAX_VALUE : 0L;
        Helpers.Tuple2<Double, Long> finalTuple = new Tuple2<>(tmpVal, 0L);
        if (nodeList == null) {//If nodelist is null then return local
            //Get local meta
            HashMap<HostMetricKey, MetaMetric> localMeta = extractMetaData(filter, localNode);
            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
            Tuple2<Double, Long> values = srvInterface.extractMonitoringQuery(localMeta.keySet(), from, to, agg);
            finalTuple = combineTuples(finalTuple, values, agg);
        } else {
            String[] nodeArray;
            if (nodeList.isEmpty()) { //Else if it is empty return for every server
                nodeArray = ignite.cluster().forServers().hostNames().toArray(new String[0]);
            } else { //Return specified nodes
                nodeArray = nodeList.toArray(new String[0]);
            }
            //Get the meta queried
            HashMap<HostMetricKey, MetaMetric> meta = extractMetaData(filter, nodeArray);
            HashMap<String, HashSet<HostMetricKey>> nodeSet = getReplicaSet(meta.keySet());
            for (Map.Entry<String, HashSet<HostMetricKey>> node : nodeSet.entrySet()) {
                if (!ignite.cluster().forServers().forHost(node.getKey()).nodes().isEmpty()) {
                    DataManagementInterface srvInterface = ignite.services(ignite.cluster().forServers().forHost(node.getKey())).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
                    Tuple2<Double, Long> values = srvInterface.extractMonitoringQuery(node.getValue(), from, to, agg);
                    finalTuple = combineTuples(finalTuple, values, agg);
                }
            }
        }
        if (finalTuple != null) {
            if (agg == 3) //Avg
                return finalTuple.first / finalTuple.second;
            else
                return finalTuple.first;
        } else return null;
    }

    @Override
    public HashMap<HostMetricKey, MetaMetric> extractMonitoringList(HashMap<String, HashSet<String>> filter, HashSet<String> nodesList) {
        HashMap<HostMetricKey, MetaMetric> result;
        if(nodesList == null){
            result = extractMetaData(filter, localNode);
        }
        else {
            String[] nodeArray;
            if (nodesList.isEmpty()) { //Else if it is empty return for every server
                nodeArray = ignite.cluster().forServers().hostNames().toArray(new String[0]);
            } else { //Return specified nodes
                nodeArray = nodesList.toArray(new String[0]);
            }
            result = extractMetaData(filter, nodeArray);
        }
        return result;
    }

    @Override
    public boolean deleteMonitoring(HashMap<String, HashSet<String>> filter, HashSet<String> nodeList) {
        if (nodeList.isEmpty()) {//If nodelist is empty
            HashMap<HostMetricKey, MetaMetric> localMeta = extractMetaData(filter, localNode);
            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
            if (srvInterface.deleteMonitoring(localMeta.keySet())) {
                StringBuilder sql = new StringBuilder(SQL_WHERE);
                localMeta.keySet().forEach((k -> sql.append(" (metricID = '").append(k.metric.metricID)
                        .append("' AND entityID = '").append(k.metric.entityID)
                        .append("' AND hostname = '").append(k.hostname).append("') OR")));
                sql.delete(sql.length() - 2, sql.length());
                getQueryValues(myMeta, "DELETE FROM METAMETRIC " + sql);
                return true;
            }
            return false;
        } else {
            HashMap<HostMetricKey, MetaMetric> replicatedMeta = extractMetaData(filter, nodeList.toArray(new String[0]));
            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
            return srvInterface.deleteMonitoring(replicatedMeta.keySet());
        }
    }

    @Override
    public HashMap<String, Boolean> extractNodes() {
        HashMap<String, Boolean> result = new HashMap<>();
        for (ClusterNode node : ignite.cluster().forServers().nodes()) {
            result.put(node.hostNames().iterator().next(), node.attribute("data.head"));
        }
        return result;
    }

    @Override
    public void setReplication(boolean replica) {
        replicate = replica;
    }

    @Override
    public void init(ServiceContext ctx) {
        System.out.println("Initializing Movement Service on node:" + ignite.cluster().localNode());
        //Get the cache that is designed in the config for the latest data
        myMeta = ignite.cache(META_CACHE_NAME);
        myReplica = ignite.cache(REPLICA_HOST_CACHE);
    }

    @Override
    public void execute(ServiceContext ctx) {
        System.out.println("Executing Movement Service on node:" + ignite.cluster().localNode());
        //Check if replication is needed after a restart
        //So we go with a full scan query
        try (QueryCursor<Cache.Entry<HostMetricKey, List<String>>> qryCursor = myReplica.query(new ScanQuery<>())) {
            for (Cache.Entry<HostMetricKey, List<String>> entry : qryCursor.getAll()) {
                if (entry.getKey().hostname.equals(ignite.cluster().forLocal().hostNames().iterator().next())) {
                    replicate = true;
                    break;
                }
            }
        }
    }

    @Override
    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Movement Service on node:" + ignite.cluster().localNode());
    }

}
