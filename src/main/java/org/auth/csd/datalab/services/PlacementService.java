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
import org.auth.csd.datalab.InfluxConnector.WriteData;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.auth.csd.datalab.ServerNodeStartup.*;
import static org.auth.csd.datalab.common.Helpers.readEnvVariable;

public class PlacementService implements PlacementInterface {


    @IgniteInstanceResource
    private Ignite ignite;
    /**
     * Reference to the cache.
     */
    private static IgniteCache<HostMetricKey, Set<String>> myReplica;
    private static IgniteCache<String, Restarts> myRestarts;
    private static final int STABLE_MAX_FAILS = 1;
    private static final int MAX_OUT_REPLICAS = 2;
    private static final long OLD_PERIOD = (1000 * 60 * 60 * 24 * 7); //1 week
    private static final long MID_PERIOD = (1000 * 60 * 60 * 24 * 2); //2 days
    private static long interval = 1800; //Seconds (30 mins default)
    private static final long PROBLEMATIC_TAG_INTERVAL = 3600; //(Seconds) Tag the node as a possible problem if it hasn't been replicated in the last XXX time period
    private static final double PROBLEMATIC_THRESHOLD = 0.01; //Threshold to identify the node as problematic (Possibly needs to be refined)
    private static volatile boolean cancelled=false; //Loop cancel

    private void startListener() {
        //Get ignite events
        IgniteEvents events = ignite.events();
        // Local listener that listens to local events (includes all node discoveries failures)
        IgnitePredicate<DiscoveryEvent> localListener = evt -> {
            //Get evt node and type
            int type = evt.type();
            String hostname = evt.eventNode().hostNames().iterator().next();
            long currTime = System.currentTimeMillis();
            boolean newHost = !myRestarts.containsKey(hostname);
            if(newHost) {
                System.out.println(hostname + " has started");
                myRestarts.put(hostname, new Restarts(currTime));
                WriteData.writeNodeStart(hostname, currTime);
            }
            else {
                Restarts tmp = myRestarts.get(hostname);
                if (type == EventType.EVT_NODE_JOINED) {
                    System.out.println(hostname + " has restarted");
                    tmp.addRestart(currTime);
                    myRestarts.put(hostname, tmp);
                    WriteData.writeNodeRestart(hostname, currTime);
                } else if (type == EventType.EVT_NODE_FAILED) {
                    System.out.println(hostname + " has failed");
                    tmp.addFail(currTime);
                    myRestarts.put(hostname, tmp);
                    WriteData.writeNodeFail(hostname, currTime);
                }
            }
            return true; // Continue listening.
        };
        // Subscribe to the discovery events that are triggered on the local node.
        events.localListen(localListener, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_FAILED);
    }

    private void clearPreviousUnstable(String hostname){
        long currTime = System.currentTimeMillis();
        //Set replication on the node to false, so it stops replicating data
        MovementInterface srvInterface = ignite.services(ignite.cluster().forHost(hostname)).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
        srvInterface.setReplication(false);
        //Remove the entries from the replicas cache
         HashSet<String> sourceSet = new HashSet<>();
         sourceSet.add(hostname);
         HashMap<HostMetricKey, MetaMetric> tmpMmetrics = srvInterface.extractMonitoringList(new HashMap<>(), sourceSet);
        if(!tmpMmetrics.isEmpty()) myReplica.removeAll(tmpMmetrics.keySet());
        //Update meta
        Restarts nodeInfo = myRestarts.get(hostname);
        //And update the replica set
        nodeInfo.replicaTo.forEach(k -> {
            Restarts tmpDest = myRestarts.get(k);
            tmpDest.removeSource(hostname);
            myRestarts.put(k, tmpDest);
            WriteData.writeReplicaStop(hostname, k, currTime);
        });
        nodeInfo.clearDestination();
        myRestarts.put(hostname, nodeInfo);
    }

    private void clearPreviousStable(String hostname){
        long currTime = System.currentTimeMillis();
        Restarts nodeInfo = myRestarts.get(hostname);
        nodeInfo.replicaFrom.forEach(k -> {
            Restarts tmpRestart = myRestarts.get(k);
            MovementInterface srvInterface = ignite.services(ignite.cluster().forHost(k)).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
            HashSet<String> sourceSet = new HashSet<>();
            sourceSet.add(k);
            HashMap<HostMetricKey, MetaMetric> tmpMmetrics = srvInterface.extractMonitoringList(new HashMap<>(), sourceSet);
            AtomicBoolean continueReplication = new AtomicBoolean(false);
            if(!tmpMmetrics.isEmpty()) {
                tmpMmetrics.keySet().forEach(metric -> {
                    Set<String> tmpDests = myReplica.get(metric);
                    tmpDests.remove(hostname);
                    if(tmpDests.isEmpty()) myReplica.remove(metric);
                    else {
                        myReplica.put(metric, tmpDests);
                        continueReplication.set(true);
                    }
                });
            }
            srvInterface.setReplication(continueReplication.get());
            tmpRestart.removeDestination(hostname);
            myRestarts.put(k, tmpRestart);
            WriteData.writeReplicaStop(k, hostname, currTime);
        });
        nodeInfo.clearSource();
        myRestarts.put(hostname, nodeInfo);
    }

    private boolean decidePlacement() {
        //First find the top-N problematic nodes
        List<NodeScore> problematicNodes = findProblematicNodes(1);
        if (!problematicNodes.isEmpty()) {
            //Then find the stable nodes
            List<NodeScore> stableNodes = findStableNodes(0, problematicNodes.stream().map(k -> k.node).collect(Collectors.toList()));
            if (!stableNodes.isEmpty()) {
                for (NodeScore source : problematicNodes) {
                    Map.Entry<NodeScore, List<HostMetricKey>> dest = findReplicaDestination(source, stableNodes);
                    if (dest != null && !ignite.cluster().forHost(source.node).nodes().isEmpty()) {
                        long currTime = System.currentTimeMillis();
                        //Replicate stuff
                        MovementInterface srvInterface = ignite.services(ignite.cluster().forHost(source.node)).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
                        srvInterface.startReplication(new HashSet<>(dest.getValue()), dest.getKey().node);
                        //Write data to influx
                        System.out.println("Replicating : " + source.node + " to " + dest.getKey().node);
                        WriteData.writeReplicaStart(source.node, dest.getKey().node, currTime);
                        //Update the last replication time and the destination
                        Restarts tmp = myRestarts.get(source.node); //No need for check. It has been completed on finding the problematic node
                        tmp.addReplication(currTime);
                        tmp.addDestination(dest.getKey().node);
                        myRestarts.put(source.node, tmp);
                        //Update the source node
                        Restarts tmp2 = myRestarts.get(dest.getKey().node); //No need for check. It has been completed on finding the stable node
                        tmp2.addSource(source.node);
                        myRestarts.put(dest.getKey().node, tmp2);
                        //Update the replica table
                        String newHost = dest.getKey().node;
                        dest.getValue().forEach(k -> {
                            Set<String> repls;
                            if(myReplica.containsKey(k)) repls = myReplica.get(k);
                            else repls = new HashSet<>();
                            repls.add(newHost);
                            myReplica.put(k, repls);
                        });
                    }
                }
                return true;
            }
        }
        return false;
    }

    private List<NodeScore> findProblematicNodes(int topN) {
        ArrayList<NodeScore> problematicNodes = new ArrayList<>();
        long currTime = System.currentTimeMillis();
        for (ClusterNode node : ignite.cluster().forServers().forRemotes().nodes()) {
            String hostname = node.hostNames().iterator().next();
            Restarts nodeFails = (myRestarts.containsKey(hostname)) ? myRestarts.get(hostname) : null;
            if(nodeFails == null) {
                System.out.println(hostname + " has started");
                myRestarts.put(hostname, new Restarts(currTime));
                WriteData.writeNodeStart(hostname, currTime);
                continue;
            }
            if (nodeFails.failTimes.isEmpty() || nodeFails.lastReplication >= (currTime - (PROBLEMATIC_TAG_INTERVAL*1000))) {
//                WriteData.writeUnstableScore(hostname, currTime, 0D);
                continue; //It is not a problematic node
            }
            //Check if it reached max capacity
//            if(getReplicatedMetricsMax(hostname) >= MAX_OUT_REPLICAS) {
            //TODO DEMO PURPOSES ONLY
            if(nodeFails.replicaTo.size() >= MAX_OUT_REPLICAS){
//                WriteData.writeUnstableScore(hostname, currTime, 0D);
                continue;
            }
            //Else get a weighted average of the time between fails
            //Start by getting the start time of the node
            long startTime = nodeFails.startTime;
            double sum = 0; //Sum the diff between fails with weights
            long oldFails = nodeFails.failTimes.stream().filter(k -> k <= currTime - OLD_PERIOD).count();
            long midFails = nodeFails.failTimes.stream().filter(k -> k > currTime - OLD_PERIOD && k <= currTime - MID_PERIOD).count();
            long newFails = nodeFails.failTimes.stream().filter(k -> k > currTime - MID_PERIOD).count();
            for (int i = 0; i < nodeFails.failTimes.size(); i++) {
                long timestamp = nodeFails.failTimes.get(i);
                double a;
                if (timestamp <= currTime - OLD_PERIOD) a = 0.5 * oldFails;
                else if (timestamp <= currTime - MID_PERIOD) a = midFails;
                else a = 2D * newFails;
                if (i == 0) sum += a / ((timestamp - startTime)/1000D);
                else sum += a / ((timestamp - nodeFails.failTimes.get(i - 1))/1000D);
            }
            System.out.println("Host: " + hostname + " - sum: " + sum);
            WriteData.writeUnstableScore(hostname, currTime, sum);
            //Add the node with the score in the map if it passes above a threshold
            if (sum >= PROBLEMATIC_THRESHOLD) {
                problematicNodes.add(new NodeScore(hostname, sum));
                //check if a problematic node is a replica destination for another node
                if(!nodeFails.replicaFrom.isEmpty()) clearPreviousStable(hostname);
            }
        }
        //Find and return the top N nodes
        if (problematicNodes.isEmpty()) return problematicNodes;
        return sortScores(topN, problematicNodes);
    }

    private List<NodeScore> findStableNodes(int topN, List<String> exclude) {
        ArrayList<NodeScore> stableNodes = new ArrayList<>();
        long currTime = System.currentTimeMillis();
        for (ClusterNode node : ignite.cluster().forServers().forRemotes().nodes()) {
            String hostname = node.hostNames().iterator().next();
            Restarts nodeInfo = (myRestarts.containsKey(hostname)) ? myRestarts.get(hostname) : null;
            if(nodeInfo == null) continue;
            if(!exclude.contains(hostname)){
                //Get number of fails
                long nodeFails = nodeInfo.failTimes.stream().filter(k -> k > currTime - MID_PERIOD).count();
                if (nodeFails > STABLE_MAX_FAILS || nodeInfo.lastReplication > currTime - (PROBLEMATIC_TAG_INTERVAL*1000)) { //Either it has failed too many times or it has been replicated in the last hour
//                    WriteData.writeStableScore(hostname, currTime, 0D);
                    continue; //If it failed many times recently exclude it
                }
                //Sum up the restarts and the fails
                double nodeRestarts = (-nodeInfo.restartTimes.size() - (nodeFails * 3D)) * ((currTime - nodeInfo.startTime)/1000D); //Get the time alive of the node minus the restarts
                WriteData.writeStableScore(hostname, currTime, nodeRestarts);
                //Add the node with the score in the map (Bigger score = better node)
                stableNodes.add(new NodeScore(hostname, nodeRestarts));
                //If the node was unstable in the past remove the replications
                if(!nodeInfo.replicaTo.isEmpty()) clearPreviousUnstable(hostname);
            }
        }
        //Find and return the top N nodes
        if (stableNodes.isEmpty()) return stableNodes;
        return sortScores(topN, stableNodes);
    }

    private Map.Entry<NodeScore, List<HostMetricKey>> findReplicaDestination(NodeScore source, List<NodeScore> dest) {
        NodeScore resultNode = null;
        List<HostMetricKey> metricsToReplicate = new ArrayList<>();
        //Get all data points for the source node
        MovementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
        HashSet<String> sourceSet = new HashSet<>();
        sourceSet.add(source.node);
        HashMap<HostMetricKey, MetaMetric> tmpMmetrics = srvInterface.extractMonitoringList(new HashMap<>(), sourceSet);
        if (!tmpMmetrics.isEmpty()) {
            Set<HostMetricKey> metrics = tmpMmetrics.keySet();
            int metricSize = metrics.size();
            //Get already replicated data points
            Map<HostMetricKey, Set<String>> replicas = myReplica.getAll(metrics);
            HashMap<String, Integer> replicatedMetrics = new HashMap<>();
            replicas.values().stream().flatMap(Set::stream).forEach(k -> replicatedMetrics.put(k, replicatedMetrics.getOrDefault(k, 0) + 1));
            AtomicInteger tmpPos = new AtomicInteger(dest.size() + 1);
            //If a node that already stores the problematic nodes data is within the top 50% of the stable nodes and has only a set of the metrics then store the rest of them there as well
            replicatedMetrics.forEach((k, v) -> {
                if (v < metricSize)
                    for (NodeScore tmp : dest)
                        if (tmp.node.equals(k)) tmpPos.set(Math.min(dest.indexOf(tmp), tmpPos.get()));
            });
            if (tmpPos.get() < dest.size() + 1) {
                //Get the diff of the metrics and store them
                resultNode = dest.get(tmpPos.get());
                NodeScore finalResultNode = resultNode;
                metrics.forEach(k -> {
                    if (!replicas.get(k).contains(finalResultNode.node)) metricsToReplicate.add(k);
                });
            } else {
                //Get iter
                Iterator<NodeScore> iter = dest.iterator();
                boolean check = false;
                while (iter.hasNext() && !check){
                    NodeScore tmp = iter.next();
                    if(!replicatedMetrics.containsKey(tmp.node)){
                        resultNode = tmp;
                        metricsToReplicate.addAll(metrics);
                        check = true;
                    }
                }
            }
            if(resultNode == null) return null;
            else return new AbstractMap.SimpleEntry<>(resultNode, metricsToReplicate);
        } else { //TODO FOR DEMO PURPOSE ONLY
            Restarts nodeFails = myRestarts.get(source.node);
            Iterator<NodeScore> iter = dest.iterator();
            boolean check = false;
            while (iter.hasNext() && !check){
                NodeScore tmp = iter.next();
                if(!nodeFails.replicaTo.contains(tmp.node)){
                    resultNode = tmp;
                    check = true;
                }
            }
            return new AbstractMap.SimpleEntry<>(resultNode, metricsToReplicate);
            //return null;
        }
    }

    private int getReplicatedMetricsMax(String hostname){
        int maxtm = 0;
        MovementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
        HashSet<String> sourceSet = new HashSet<>();
        sourceSet.add(hostname);
        HashMap<HostMetricKey, MetaMetric> tmpMmetrics = srvInterface.extractMonitoringList(new HashMap<>(), sourceSet);
        if (!tmpMmetrics.isEmpty()) {
            Set<HostMetricKey> metrics = tmpMmetrics.keySet();
            //Get already replicated data points
            Map<HostMetricKey, Set<String>> replicas = myReplica.getAll(metrics);
            for (Set<String> i: replicas.values()){
                if(i.size() >= maxtm) maxtm = i.size();
            }
            return maxtm;
        }
        return maxtm;
    }

    @NotNull
    private List<NodeScore> sortScores(int topN, ArrayList<NodeScore> nodes) {
        Comparator<NodeScore> comparator = Comparator.comparingDouble(k -> k.score);
        nodes.sort(comparator.reversed());
        if (topN > 0) return nodes.subList(0, topN);
        else return nodes;
    }

    public void init(ServiceContext ctx) {
        System.out.println("Initializing Placement Service on node:" + ignite.cluster().localNode());
        //Placement interval
        String tmpInterval = readEnvVariable("PLACEMENT_INTERVAL");
        if (tmpInterval != null) interval = Long.parseLong(tmpInterval);
        myReplica = ignite.cache(REPLICA_HOST_CACHE);
        myRestarts = ignite.cache(RESTART_CACHE);
    }

    public void execute(ServiceContext ctx) throws InterruptedException {
        System.out.println("Executing Placement Service on node:" + ignite.cluster().localNode());
        //Start the event listener
        startListener();
        System.out.println("Started listener");
        //Every X time period execute the placement method (maybe streaming outlier detection)?
        Thread.sleep(60000); //Wait a minute before starting the placement stuff
        do {
            //Start placement checks
            System.out.println("Placement decision");
            decidePlacement();
            System.out.println("Decision taken");
            if(cancelled) break;
            Thread.sleep(interval * 1000);
        } while (true);

    }

    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Placement Service on node:" + ignite.cluster().localNode());
        cancelled=true;
    }

}
