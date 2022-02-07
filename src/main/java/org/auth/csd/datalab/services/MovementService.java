package org.auth.csd.datalab.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteBiPredicate;
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
import org.auth.csd.datalab.common.models.values.Metric;
import org.auth.csd.datalab.common.models.values.TimedMetric;

import java.util.*;
import java.util.stream.Collectors;

import static org.auth.csd.datalab.ServerNodeStartup.*;
import static org.auth.csd.datalab.common.Helpers.combineTuples;
import static org.auth.csd.datalab.common.Helpers.getQueryValues;

public class MovementService implements MovementInterface {

    @IgniteInstanceResource
    private Ignite ignite;
    /**
     * Reference to the cache.
     */
    private static IgniteCache<HostMetricKey, MetaMetric> myMeta;
    private static IgniteCache<HostMetricKey, List<String>> myReplica;
    private static boolean replicate = false;

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
        String hostWhere = (hostname.length == 1) ? " (hostname = '" + hostname[0] + "') " : " (hostname IN ('" + String.join("', '",hostname) + "')) ";
        String finalWhere = (!where.isEmpty()) ? " WHERE " + hostWhere + " AND (" + String.join(") AND (", where) + ") " : " WHERE " + hostWhere;
        try (QueryCursor<List<?>> cursor = getQueryValues(myMeta, select + from + finalWhere)) {
            metaTransform(result, cursor);
        }
        return result;
    }

    private void replicateNewMonitoring(HashMap<MetricKey, InputJson> metrics){
        HashMap<String, HashMap<MetricKey, InputJson>> replicas = new HashMap<>();
        for(MetricKey key: metrics.keySet()){
            List<String> remotes = myReplica.get(new HostMetricKey(key, localNode));
            if(remotes != null) {
                remotes.forEach(k -> {
                    if(replicas.containsKey(k)){
                        HashMap<MetricKey, InputJson> tmpMetric = replicas.get(k);
                        tmpMetric.put(key, metrics.get(key));
                        replicas.put(k, tmpMetric);
                    }else replicas.put(k, new HashMap<MetricKey, InputJson>(){{put(key, metrics.get(key));}});
                });
            }
        }
        replicas.forEach((k,v) -> {
            if(ignite.cluster().forServers().forHost(k).nodes().size() > 0){
                DataManagementInterface tmpInterface = ignite.services(ignite.cluster().forServers().forHost(k)).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
                tmpInterface.ingestMonitoring(v, k);
            }
        });
    }

    @Override
    public void ingestMonitoring(HashMap<MetricKey, InputJson> metrics){
        //First check if they exist in meta cache
        for (Map.Entry<MetricKey, InputJson> entry : metrics.entrySet()) {
            myMeta.putIfAbsent(new HostMetricKey(entry.getKey(), localNode), new MetaMetric(entry.getValue()));
        }
        //First store them in local node
        DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
        srvInterface.ingestMonitoring(metrics, localNode);
        //Then for each remote node call ingestMonitoring(metrics, localNode) service if there is a need to replicate
        if(replicate) replicateNewMonitoring(metrics);
    }

    @Override
    public HashMap<String, HashMap<MetricKey, Monitoring>> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to, HashSet<String> nodeList) {
        HashMap<String, HashMap<MetricKey, Monitoring>> result = new HashMap<>();
        if(nodeList.isEmpty()){//If nodelist is empty
            //Get local meta
            HashMap<HostMetricKey, MetaMetric> localMeta = extractMetaData(filter, localNode);
            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
            HashMap<HostMetricKey, List<TimedMetric>> values = srvInterface.extractMonitoring(localMeta.keySet(), from, to);
            //Create tmp result
            HashMap<MetricKey, Monitoring> tmpRes = new HashMap<>();
            localMeta.forEach((k,v) -> tmpRes.put(k.metric, new Monitoring(v, values.getOrDefault(k, new ArrayList<>()))));
            result.put(localNode, tmpRes);
        }else{
            //Get the meta queried
            HashMap<HostMetricKey, MetaMetric> meta = extractMetaData(filter, nodeList.toArray(new String[0]));
            HashMap<String, HashSet<HostMetricKey>> metricsPerNode =  new HashMap<>();
            HashSet<String> notedNodes = new HashSet<>();
            for(HostMetricKey key : meta.keySet()){
                //If the metric is local then continue without checking replicas
                if(key.hostname.equals(localNode)) {
                    //Add the hostname to the list
                    notedNodes.add(localNode);
                    HashSet<HostMetricKey> tmp = metricsPerNode.getOrDefault(localNode, new HashSet<>());
                    tmp.add(key);
                    metricsPerNode.put(localNode, tmp);
                    continue;
                }
                //Get replicas
                List<String> tmpReplicas = myReplica.get(key);
                //If replicas contain the local node, get just that
                if(tmpReplicas != null && tmpReplicas.contains(localNode)) {
                    //Add the hostname to the list
                    notedNodes.add(localNode);
                    HashSet<HostMetricKey> tmp = metricsPerNode.getOrDefault(localNode, new HashSet<>());
                    tmp.add(key);
                    metricsPerNode.put(localNode, tmp);
                    continue;
                }
                //If there are no replicas then it is a special case and the host of the key needs to be included
                if(tmpReplicas == null) notedNodes.add(key.hostname);
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
            System.out.println(notedNodes);
            System.out.println(metricsPerNode);
            //After finding the node - metrics lists continue until the minimum set is found. (start by checking the ones with the biggest lists)
            //Start with the noted nodes and exclude their data from the other nodes
        }

        return result;
    }

    @Override
    public Double extractMonitoringQuery(HashMap<String, HashSet<String>> filter, Long from, Long to, HashSet<String> nodeList, int agg) {
        Double tmpVal = (agg == 1) ? Double.MAX_VALUE : 0L;
        Helpers.Tuple2<Double, Long> finalTuple = new Tuple2<>(tmpVal,0L);
        if(nodeList.isEmpty()){//If nodelist is empty
            //Get local meta
            HashMap<HostMetricKey, MetaMetric> localMeta = extractMetaData(filter, localNode);
            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
            Tuple2<Double,Long> values = srvInterface.extractMonitoringQuery(localMeta.keySet(), from, to, agg);
            finalTuple = combineTuples(finalTuple, values, agg);
        }else {
            //TODO Check if local node has remote data and get them if necessary
//            for (String node : nodeList){
//                //Check if the node is not available or does not exist
//                if(ignite.cluster().forServers().forHost(node).nodes().size() == 0) continue;
//                HashMap<MetricKey, Monitoring> values;
//                DataManagementInterface srvInterface = ignite.services(ignite.cluster().forServers().forHost(node)).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
//                Tuple2<Double, Long> tmp = srvInterface.extractMonitoringQuery(filter, from, to, agg);
//                finalTuple = combineTuples(finalTuple, tmp, agg);
//            }
        }
        if(finalTuple != null) {
            if (agg == 3) //Avg
                return finalTuple.first / finalTuple.second;
            else
                return finalTuple.first;
        }else return null;
    }

    @Override
    public HashMap<MetricKey, MetaMetric> extractMonitoringList(HashMap<String, HashSet<String>> filter) {
        //TODO Check on cluster and maybe add hostnames on the request
        String[] hostnames = ignite.cluster().forServers().hostNames().toArray(new String[0]);
        System.out.println(String.join(",",hostnames));
        HashMap<HostMetricKey, MetaMetric> result = extractMetaData(filter, hostnames);
        System.out.println(result);
        return null;
    }

    @Override
    public Boolean deleteMonitoring(HashMap<String, HashSet<String>> filter, HashSet<String> nodeList) {
        if(nodeList.isEmpty()) {//If nodelist is empty
            HashMap<HostMetricKey, MetaMetric> localMeta = extractMetaData(filter, localNode);
            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
            if (srvInterface.deleteMonitoring(localMeta.keySet())) {
                StringBuilder sql = new StringBuilder(" WHERE ");
                localMeta.keySet().forEach((k -> sql.append(" (metricID = '").append(k.metric.metricID)
                        .append("' AND entityID = '").append(k.metric.entityID)
                        .append("' AND hostname = '").append(k.hostname).append("') OR")));
                sql.delete(sql.length() - 2, sql.length());
                getQueryValues(myMeta, "DELETE FROM METAMETRIC " + sql);
                return true;
            }
            return false;
        }else{
            //TODO change delete to also remove replicated entries to remote nodes
            return false;
        }
    }

    @Override
    public HashMap<String, Boolean> extractNodes() {
        HashMap<String, Boolean> result = new HashMap<>();
        for (ClusterNode node : ignite.cluster().forServers().nodes()){
            result.put(node.hostNames().toString(), node.attribute("data.head"));
        }
        return result;
    }

    @Override
    public void init(ServiceContext ctx) {
        System.out.println("Initializing Movement Service on node:" + ignite.cluster().localNode());
        //Get the cache that is designed in the config for the latest data
        myMeta = ignite.cache(metaCacheName);
        myReplica = ignite.cache(replicaHostCache);
        HostMetricKey test1 = new HostMetricKey("metr1", "as1", "puma.csd.auth.gr");
        ArrayList<String> test = new ArrayList<>();
        test.add("datalab-toliopoulos.csd.auth.gr");
        myReplica.put(test1, test);
    }

    @Override
    public void execute(ServiceContext ctx) {
        System.out.println("Executing Movement Service on node:" + ignite.cluster().localNode());
        //Check if replication is needed after a restart
        IgniteBiPredicate<HostMetricKey, List<Object>> filter = (key, val) -> Objects.equals(key.hostname, localNode);
        if(myReplica.query(new ScanQuery<>(filter)).getAll().size() > 0) replicate = true;
    }

    @Override
    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Movement Service on node:" + ignite.cluster().localNode());
    }

}
