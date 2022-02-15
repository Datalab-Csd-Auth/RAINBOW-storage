package org.auth.csd.datalab.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.interfaces.MovementInterface;
import org.auth.csd.datalab.common.interfaces.PlacementInterface;
import org.auth.csd.datalab.common.models.NodeScore;
import org.auth.csd.datalab.common.models.Restarts;
import org.auth.csd.datalab.common.models.keys.HostMetricKey;
import org.auth.csd.datalab.common.models.values.MetaMetric;
import org.jetbrains.annotations.NotNull;
import org.w3c.dom.Node;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.auth.csd.datalab.ServerNodeStartup.*;

public class PlacementService implements PlacementInterface {


    @IgniteInstanceResource
    private Ignite ignite;
    /**
     * Reference to the cache.
     */
    private static IgniteCache<HostMetricKey, List<String>> myReplica;
    private static HashMap<String, Restarts> restarts;
    private static HashMap<String, Restarts> fails;
    private static HashMap<String, Integer> replicas; //This holds the reverse list of replicas, i.e. nodes that have replicated data and the number of ignite instances that replicate to this node
    private static final int maxReplicas = 3;
    private static final int stableMaxFails = 1;
    private static final long oldPeriod = (1000 * 60 * 60 * 24 * 7); //1 week
    private static final long midPeriod = (1000 * 60 * 60 * 24 * 2); //2 days

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
        List<NodeScore> problematicNodes = findProblematicNodes(1);
        if (problematicNodes != null) {
            //Then find the stable nodes
            List<NodeScore> stableNodes = findStableNodes(0, problematicNodes.stream().map(k -> k.node).collect(Collectors.toList()));
            if (stableNodes != null) {
                for (NodeScore source : problematicNodes) {
                    Map.Entry<NodeScore, List<HostMetricKey>> dest = findReplicaDestination(source, stableNodes);
                    if(ignite.cluster().forHost(dest.getKey().node).nodes().size() > 0){
                        //Replicate stuff
                        MovementInterface srvInterface = ignite.services(ignite.cluster().forHost(dest.getKey().node)).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
                        srvInterface.startReplication(new HashSet<>(dest.getValue()), dest.getKey().node);
                        //Update the replica table
                        dest.getValue().forEach(k -> {
                            List<String> repls = myReplica.get(k);
                            repls.add(dest.getKey().node);
                            myReplica.put(k, repls);
                        });
                    }
                }
            }
        }
    }


    private Map.Entry<NodeScore, List<HostMetricKey>> findReplicaDestination(NodeScore source, List<NodeScore> dest) {
        NodeScore resultNode;
        List<HostMetricKey> metricsToReplicate = new ArrayList<>();
        //Get all data points for the source node
        MovementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
        HashSet<String> sourceSet = new HashSet<>();
        sourceSet.add(source.node);
        HashSet<HostMetricKey> metrics = (HashSet<HostMetricKey>) srvInterface.extractMonitoringList(new HashMap<>(), sourceSet).keySet();
        int metricSize = metrics.size();
        //Get already replicated data points
        Map<HostMetricKey, List<String>> replicas = myReplica.getAll(metrics);
        HashMap<String, Integer> replicatedMetrics = new HashMap<>();
        replicas.values().stream().flatMap(List::stream).forEach(k -> replicatedMetrics.put(k, replicatedMetrics.getOrDefault(k,0) + 1));
        AtomicInteger tmpPos = new AtomicInteger(dest.size()+1);
        //If a node that already stores the problematic nodes data is within the top 50% of the stable nodes and has only a set of the metrics then store the rest of them there as well
        replicatedMetrics.forEach((k,v) -> {
            if(v < metricSize)
                for (NodeScore tmp : dest)
                    if(tmp.node.equals(k)) tmpPos.set(Math.min(dest.indexOf(tmp), tmpPos.get()));
        });
        if (tmpPos.get() < dest.size()+1) {
            //Get the diff of the metrics and store them
            resultNode = dest.get(tmpPos.get());
            NodeScore finalResultNode = resultNode;
            metrics.forEach(k -> {
                if(!replicas.get(k).contains(finalResultNode.node)) metricsToReplicate.add(k);
            });
        }else{
            //Choose the first one and replicate everything
            resultNode = dest.get(0);
            metricsToReplicate.addAll(metrics);
        }
        Map.Entry<NodeScore, List<HostMetricKey>> res = new AbstractMap.SimpleEntry<>(resultNode, metricsToReplicate);
        return res;
    }

    private List<NodeScore> findStableNodes(int topN, List<String> exclude) {
        ArrayList<NodeScore> stableNodes = new ArrayList<>();
        long currTime = System.currentTimeMillis();
        for (ClusterNode node : ignite.cluster().forServers().forRemotes().nodes()) {
            String hostname = node.hostNames().iterator().next();
            if (exclude.contains(hostname)) continue;
            int replications = replicas.getOrDefault(hostname, 0);
            if (replications > maxReplicas) continue;
            //Get number of fails
            int nodeFails = (int) fails.getOrDefault(hostname, new Restarts()).startTimes.stream().filter(k -> k > currTime - midPeriod).count();
            if (nodeFails > stableMaxFails) continue; //If it failed many times recently exclude it
            //Sum up the restarts and the fails
            Restarts tmpRestart = restarts.getOrDefault(hostname, new Restarts(currTime));
            double nodeRestarts = (-tmpRestart.restarts - (nodeFails * 3D)) * (currTime - tmpRestart.startTimes.get(0)) / (replications+1); //Get the time alive of the node minus the restarts
            //Add the node with the score in the map (Bigger score = better node)
            stableNodes.add(new NodeScore(hostname, nodeRestarts));
        }
        //Find and return the top N nodes
        if (stableNodes.isEmpty()) return null;
        return sortScores(topN, stableNodes);
    }

    private List<NodeScore>findProblematicNodes(int topN) {
        ArrayList<NodeScore> problematicNodes = new ArrayList<>();
        long currTime = System.currentTimeMillis();
        for (ClusterNode node : ignite.cluster().forServers().forRemotes().nodes()) {
            String hostname = node.hostNames().iterator().next();
            //get number of fails and timestamps between them
            Restarts nodeFails = fails.getOrDefault(hostname, null);
            if (nodeFails == null || nodeFails.restarts < 1) continue; //It is not a problematic node
            //Else get a weighted average of the time between fails
            //Start by getting the start time of the node
            long startTime = (restarts.containsKey(hostname)) ? restarts.get(hostname).startTimes.get(0) : nodeFails.startTimes.get(0);
            double sum = 0; //Sum the diff between fails with weights
            long oldFails = nodeFails.startTimes.stream().filter(k -> k <= currTime - oldPeriod).count();
            long midFails = nodeFails.startTimes.stream().filter(k -> k > currTime - oldPeriod && k <= currTime - midPeriod).count();
            long newFails = nodeFails.startTimes.stream().filter(k -> k > currTime - midPeriod).count();
            for (int i = 0; i < nodeFails.startTimes.size(); i++) {
                long timestamp = nodeFails.startTimes.get(i);
                double a;
                if (timestamp <= currTime - oldPeriod) a = 0.5 * oldFails;
                else if (timestamp <= currTime - midPeriod) a = midFails;
                else a = 2 * newFails;
                if (i == 0) sum += a * (timestamp - startTime);
                else sum += a * (timestamp - nodeFails.startTimes.get(i - 1));
            }
            //Add the node with the score in the map
            problematicNodes.add(new NodeScore(hostname, sum));
        }
        //Find and return the top N nodes
        if (problematicNodes.isEmpty()) return null;
        return sortScores(topN, problematicNodes);
    }

    @NotNull
    private List<NodeScore> sortScores(int topN, ArrayList<NodeScore> nodes) {
        Comparator<NodeScore> comparator = Comparator.comparingDouble(k -> k.score);
        nodes.sort(comparator.reversed());
        if (topN > 0) return nodes.subList(0, topN);
        else return  nodes;
    }

    public void init(ServiceContext ctx) {
        System.out.println("Initializing Placement Service on node:" + ignite.cluster().localNode());
        //Get the cache that is designed in the config for the latest data
        restarts = new HashMap<>();
        fails = new HashMap<>();
        replicas = new HashMap<>();
        //TODO Maybe initialize it if the node restarts (local cache)
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
