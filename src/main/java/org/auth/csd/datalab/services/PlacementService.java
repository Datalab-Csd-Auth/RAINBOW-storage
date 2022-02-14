package org.auth.csd.datalab.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.interfaces.MovementInterface;
import org.auth.csd.datalab.common.interfaces.PlacementInterface;
import org.auth.csd.datalab.common.models.Restarts;
import org.auth.csd.datalab.common.models.keys.HostMetricKey;
import org.auth.csd.datalab.common.models.values.MetaMetric;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

import static org.auth.csd.datalab.ServerNodeStartup.*;

public class PlacementService implements PlacementInterface {


    @IgniteInstanceResource
    private Ignite ignite;
    /**
     * Reference to the cache.
     */
    private static IgniteCache<HostMetricKey, MetaMetric> myMeta;
    private static IgniteCache<HostMetricKey, List<String>> myReplica;
    private static HashMap<String, Restarts> restarts;
    private static HashMap<String, Restarts> fails;
    private static HashMap<String, Integer> replicas; //This holds the reverse list of replicas, i.e. nodes that have replicated data and the number of ignite instances that replicate to this node
    private static final int maxReplicas = 3;
    private static final int stableMaxFails = 1;
    private static final long oldPeriod = (1000 * 60 * 60 * 24 * 7);
    private static final long midPeriod = (1000 * 60 * 60 * 24 * 2);

    private void startListener() {
        //Get ignite events
        IgniteEvents events = ignite.events();
        // Local listener that listens to local events (includes all node discoveries failures)
        IgnitePredicate<DiscoveryEvent> localListener = evt -> {
            //Get evt node and type
            int type = evt.type();
            String hostname = evt.eventNode().hostNames().iterator().next();
            if (type == EventType.EVT_NODE_JOINED) {
                Restarts tmp;
                if (restarts.containsKey(hostname)) {
                    tmp = restarts.get(hostname);
                    tmp.addRestart(System.currentTimeMillis());
                } else tmp = new Restarts(System.currentTimeMillis());
                restarts.put(hostname, tmp);
            } else if (type == EventType.EVT_NODE_FAILED) {
                Restarts tmp = fails.getOrDefault(hostname, new Restarts());
                tmp.addRestart(System.currentTimeMillis());
                fails.put(hostname, tmp);
            }
            //TODO maybe do something with EVT_NODE_LEFT?
            return true; // Continue listening.
        };
        // Subscribe to the discovery events that are triggered on the local node.
        events.localListen(localListener, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED);
    }

    //TODO make 2 requests so that external services can replicate stuff (valentina)
    //1 - get a table of possible candidates for a node
    //2 - replicate the node's data to the chosen candidate

    private void decidePlacement() {
        //First find the top-N problematic nodes
        List<String> problematicNodes = findProblematicNodes(1);
        if (problematicNodes != null) {
            //Then find the stable nodes
            List<String> stableNodes = findStableNodes(0, problematicNodes);
            if (stableNodes != null) {
                for (String source : problematicNodes) {
                    String dest = findReplicaDestination(source, stableNodes);
                }
            }
        }

        //TODO rebalance service will write to replica cache the metrics
    }


    private String findReplicaDestination(String source, List<String> dest) {
        //TODO function and decide which metrics go to which node
        //Get all data points for the source node
        MovementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
        HashSet<String> sourceSet = new HashSet<>();
        sourceSet.add(source);
        HashSet<HostMetricKey> metrics = (HashSet<HostMetricKey>) srvInterface.extractMonitoringList(new HashMap<>(), sourceSet).keySet();
        //Get already replicated data points
        Map<HostMetricKey, List<String>> replicas = myReplica.getAll(metrics);
        List<String> nodes = replicas.values().stream().flatMap(List::stream).distinct().collect(Collectors.toList());


        return null;
    }

    private List<String> findStableNodes(int topN, List<String> exclude) {
        HashMap<String, Double> stableNodes = new HashMap<>();
        long currTime = System.currentTimeMillis();
        for (ClusterNode node : ignite.cluster().forServers().forRemotes().nodes()) {
            String hostname = node.hostNames().iterator().next();
            if (exclude.contains(hostname)) continue;
            int replications = replicas.getOrDefault(hostname, 0);
            if (replications > maxReplicas) continue;
            //Get number of fails
            int nodeFails = (int) fails.getOrDefault(hostname, new Restarts()).startTimes.stream().filter(k -> k > System.currentTimeMillis() - midPeriod).count();
            if (nodeFails > stableMaxFails) continue; //If it failed many times recently exclude it
            //Sum up the restarts and the fails
            Restarts tmpRestart = restarts.getOrDefault(hostname, new Restarts(System.currentTimeMillis()));
            double nodeRestarts = (-tmpRestart.restarts - (nodeFails * 3D)) * (currTime - tmpRestart.startTimes.get(0)) / replications; //Get the time alive of the node minus the restarts
            //Add the node with the score in the map (Bigger score = better node)
            stableNodes.put(hostname, nodeRestarts);
        }
        //Find and return the top N nodes
        if (stableNodes.isEmpty()) return null;
        return getTopFromList(topN, stableNodes);
    }

    private List<String> findProblematicNodes(int topN) {
        HashMap<String, Double> problematicNodes = new HashMap<>();
        for (ClusterNode node : ignite.cluster().forServers().forRemotes().nodes()) {
            String hostname = node.hostNames().iterator().next();
            //get number of fails and timestamps between them
            Restarts nodeFails = fails.getOrDefault(hostname, null);
            if (nodeFails == null || nodeFails.restarts < 1) continue; //It is not a problematic node
            //Else get a weighted average of the time between fails
            //Start by getting the start time of the node
            long startTime = (restarts.containsKey(hostname)) ? restarts.get(hostname).startTimes.get(0) : nodeFails.startTimes.get(0);
            long currTime = System.currentTimeMillis();
            double sum = 0; //Sum the diff between fails with weights
            for (int i = 0; i < nodeFails.startTimes.size(); i++) {
                long timestamp = nodeFails.startTimes.get(i);
                double a;
                if (timestamp <= currTime - oldPeriod) a = 0.5;
                else if (timestamp <= currTime - midPeriod) a = 1;
                else a = 2;
                if (i == 0) sum += a * (timestamp - startTime);
                else sum += a * (timestamp - nodeFails.startTimes.get(i - 1));
            }
            //Add the node with the score in the map
            problematicNodes.put(hostname, sum / nodeFails.restarts);
        }
        //Find and return the top N nodes
        if (problematicNodes.isEmpty()) return null;
        return getTopFromList(topN, problematicNodes);
    }

    @NotNull
    private List<String> getTopFromList(int topN, HashMap<String, Double> problematicNodes) {
        List<Map.Entry<String, Double>> list = new ArrayList<>(problematicNodes.entrySet());
        list.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));
        if (topN > 0) return list.subList(0, topN).stream().map(Map.Entry::getKey).collect(Collectors.toList());
        else return list.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    public void init(ServiceContext ctx) {
        System.out.println("Initializing Placement Service on node:" + ignite.cluster().localNode());
        //Get the cache that is designed in the config for the latest data
        restarts = new HashMap<>();
        fails = new HashMap<>();
        //TODO Maybe initialize it if the node restarts
        replicas = new HashMap<>();
        myMeta = ignite.cache(metaCacheName);
        myReplica = ignite.cache(replicaHostCache);
    }

    public void execute(ServiceContext ctx) throws InterruptedException {
        System.out.println("Executing Placement Service on node:" + ignite.cluster().localNode());
        //Start the event listener
        startListener();
        //Every X time period execute the placement method (maybe streaming outlier detection)?
        do {
            decidePlacement();
            Thread.sleep(10000);
        } while (true);

    }

    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Placement Service on node:" + ignite.cluster().localNode());
    }

}
