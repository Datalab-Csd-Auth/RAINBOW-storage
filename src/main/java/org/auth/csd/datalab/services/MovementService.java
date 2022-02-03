package org.auth.csd.datalab.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.Helpers;
import org.auth.csd.datalab.common.interfaces.DataManagementInterface;
import org.auth.csd.datalab.common.interfaces.MovementInterface;
import org.auth.csd.datalab.common.models.InputJson;
import org.auth.csd.datalab.common.models.Monitoring;
import org.auth.csd.datalab.common.models.Replica;
import org.auth.csd.datalab.common.models.ReplicaHost;
import org.auth.csd.datalab.common.models.keys.MetricKey;
import org.auth.csd.datalab.common.models.keys.ReplicaMetricKey;
import org.auth.csd.datalab.common.models.keys.ReplicaMetricTimeKey;
import org.auth.csd.datalab.common.models.values.MetaMetric;
import org.auth.csd.datalab.common.models.values.Metric;
import org.auth.csd.datalab.common.models.values.TimedMetric;
import org.auth.csd.datalab.common.Helpers.Tuple2;

import java.util.HashMap;
import java.util.HashSet;

import static org.auth.csd.datalab.ServerNodeStartup.*;
import static org.auth.csd.datalab.common.Helpers.combineTuples;

public class MovementService implements MovementInterface {

    @IgniteInstanceResource
    private Ignite ignite;
    private HashMap<String, Replica> localReplicas = null;
    private HashMap<String, Replica> remoteReplicas = null;
    /**
     * Reference to the cache.
     */
    private IgniteCache<String, ReplicaHost> myReplicaHosts;

    @Override
    public void ingestMonitoring(HashMap<MetricKey, InputJson> metrics){
        //First store them in local node
        DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
        srvInterface.ingestMonitoring(metrics);
        //TODO Check if there is a need to replicate them in a remote node
    }

    @Override
    public HashMap<String, HashMap<MetricKey, Monitoring>> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to, HashSet<String> nodeList) {
        HashMap<String, HashMap<MetricKey, Monitoring>> result = new HashMap<>();
        //TODO Check if local node has remote data
        if(nodeList.isEmpty()) nodeList.add(localNode);
        for (String node : nodeList){
            //Check if the node is not available or does not exist
            if(ignite.cluster().forServers().forHost(node).nodes().size() == 0) continue;
            HashMap<MetricKey, Monitoring> values;
            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forServers().forHost(node)).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
            values = srvInterface.extractMonitoring(filter, from, to);
            result.put(node, values);
        }
        return result;
    }

    @Override
    public Double extractMonitoringQuery(HashMap<String, HashSet<String>> filter, Long from, Long to, HashSet<String> nodeList, int agg) {
        Double tmpVal = (agg == 1) ? Double.MAX_VALUE : 0L;
        Helpers.Tuple2<Double, Long> finalTuple = new Tuple2<>(tmpVal,0L);
        //TODO Check if local node has remote data
        if(nodeList.isEmpty()) nodeList.add(localNode);
        for (String node : nodeList){
            //Check if the node is not available or does not exist
            if(ignite.cluster().forServers().forHost(node).nodes().size() == 0) continue;
            HashMap<MetricKey, Monitoring> values;
            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forServers().forHost(node)).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
            Tuple2<Double, Long> tmp = srvInterface.extractMonitoringQuery(filter, from, to, agg);
            finalTuple = combineTuples(finalTuple, tmp, agg);
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
            result.putAll(srvInterface.extractMeta(filter));
        }
        return result;
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
        myReplicaHosts = ignite.cache(replicaHostCache);
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
