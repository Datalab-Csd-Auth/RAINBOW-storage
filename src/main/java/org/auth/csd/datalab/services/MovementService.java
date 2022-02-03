package org.auth.csd.datalab.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
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

    private HashMap<HostMetricKey, MetaMetric> extractMetaData(HashMap<String, HashSet<String>> filter, HashSet<String> hostnames) {
        HashMap<HostMetricKey, MetaMetric> result = new HashMap<>();
        String sql = "SELECT " +
                "metricID, entityID, entityType, name, units, desc, groupName, minVal, maxVal, higherIsBetter, podUUID, podName, podNamespace, containerID, containerName, hostname " +
                "FROM METAMETRIC " +
                "WHERE hostname  IN ('" + String.join("'),('",hostnames) + "')";
        try (QueryCursor<List<?>> cursor = getQueryValues(myMeta, sql)) {
            for (List<?> row : cursor) {
                HostMetricKey key = new HostMetricKey(row.get(0).toString(), row.get(1).toString(), row.get(15).toString());
                MetaMetric value = new MetaMetric(row.get(2).toString(), row.get(3).toString(), row.get(4).toString(), row.get(5).toString(),
                        row.get(6).toString(), (double) row.get(7), (double) row.get(8), (boolean) row.get(9),
                        row.get(10).toString(), row.get(11).toString(), row.get(12).toString(), row.get(13).toString(),
                        row.get(14).toString());
                result.put(key, value);
            }
        }
        return result;
    }

    private HashMap<HostMetricKey, MetaMetric> extractMetaData(HashMap<String, HashSet<String>> filter, String hostname) {
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
        String finalWhere = (!where.isEmpty()) ? " WHERE (hostname = '" + hostname + "') AND (" + String.join(") AND (", where) + ") " : " WHERE (hostname = '" + hostname + "')";
        try (QueryCursor<List<?>> cursor = getQueryValues(myMeta, select + from + finalWhere)) {
            for (List<?> row : cursor) {
                HostMetricKey key = new HostMetricKey(row.get(0).toString(), row.get(1).toString(), row.get(15).toString());
                MetaMetric value = new MetaMetric(row.get(2).toString(), row.get(3).toString(), row.get(4).toString(), row.get(5).toString(),
                        row.get(6).toString(), (double) row.get(7), (double) row.get(8), (boolean) row.get(9),
                        row.get(10).toString(), row.get(11).toString(), row.get(12).toString(), row.get(13).toString(),
                        row.get(14).toString());
                result.put(key, value);
            }
        }
        return result;
    }

    @Override
    public void ingestMonitoring(HashMap<MetricKey, InputJson> metrics){
        //First check if they exist in meta cache
        for (Map.Entry<MetricKey, InputJson> entry : metrics.entrySet()) {
            myMeta.putIfAbsent(new HostMetricKey(entry.getKey(), localNode), new MetaMetric(entry.getValue()));
        }
        //First store them in local node
        DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
        srvInterface.ingestMonitoring(metrics);
        //TODO Check if there is a need to replicate them in a remote node
        //for each remote node call ingestMonitoring(metrics, localNode) service
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
            //TODO Check if local node has remote data and get them if necessary
//        if(nodeList.isEmpty()) nodeList.add(localNode);
//        for (String node : nodeList){
//            //Check if the node is not available or does not exist
//            if(ignite.cluster().forServers().forHost(node).nodes().size() == 0) continue;
//            HashMap<MetricKey, Monitoring> values;
////            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forServers().forHost(node)).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
////            values = srvInterface.extractMonitoring(filter, from, to);
////            result.put(node, values);
//        }
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
        HashMap<MetricKey, MetaMetric> result = new HashMap<>();
        for (ClusterNode node : ignite.cluster().forServers().nodes()){
            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forServers().forNode(node)).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
//            result.putAll(srvInterface.extractMeta(filter));
        }
        return result;
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
    }

    @Override
    public void execute(ServiceContext ctx) {
        System.out.println("Executing Movement Service on node:" + ignite.cluster().localNode());
    }

    @Override
    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Movement Service on node:" + ignite.cluster().localNode());
    }

}
