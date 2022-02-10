package org.auth.csd.datalab.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.interfaces.RebalanceInterface;
import org.auth.csd.datalab.common.models.keys.HostMetricKey;
import org.auth.csd.datalab.common.models.values.MetaMetric;

import java.util.*;

import static org.auth.csd.datalab.ServerNodeStartup.metaCacheName;
import static org.auth.csd.datalab.ServerNodeStartup.replicaHostCache;

public class RebalanceService implements RebalanceInterface {


    @IgniteInstanceResource
    private Ignite ignite;
//    /** Reference to the cache. */
    private static IgniteCache<HostMetricKey, MetaMetric> myMeta;
    private static IgniteCache<HostMetricKey, List<String>> myReplica;

    public void init(ServiceContext ctx) {
        System.out.println("Initializing Rebalance Service on node:" + ignite.cluster().localNode());
        //Get the cache that is designed in the config for the latest data
        myMeta = ignite.cache(metaCacheName);
        myReplica = ignite.cache(replicaHostCache);
    }

    public void execute(ServiceContext ctx) {
        System.out.println("Executing Rebalance Service on node:" + ignite.cluster().localNode());
        //Every X time period execute the rebalance method (maybe streaming outlier detection)?

    }

    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Rebalance Service on node:" + ignite.cluster().localNode());
    }

    //TODO make 2 requests so that external services can replicate stuff (valentina)
    //1 - get a table of possible candidates for a node
    //2 - replicate the node's data to the chosen candidate

    private void rebalanceNodes(){
        //TODO First find problematic nodes

        //TODO Second find possible nodes for replication

        //Get info on metrics
        ClusterMetrics metrics = ignite.cluster().forServers().metrics();
        double cpuload = metrics.getAverageCpuLoad();
    }

    /*
    @Override
    public Void rebalanceData(Set<String> keys) {
        //Start by checking if state is partitioned/replicated and remote node still exists
        if((state == 1 || state == 2) && ignite.cluster().forServers().forNodeId(externalNode).nodes().size() == 0) state = 0;
        //Start by checking if cache is replicated/partitioned
        if(state == 1){
            replicateData();
        }else if(state == 2){
            partitionData();
        }else if(state == 0){
            //Check if partitioning or replication is necessary
            //Get info on local metrics
            ClusterMetrics metrics = ignite.cluster().forLocal().metrics();
            double cpuload = metrics.getAverageCpuLoad();
            //TODO better conditions AND way to find new nodes
            if(cpuload >= 0.5){ //Too much load, replicate data for safety
                System.out.println("Replication needed");;
                if(ignite.cluster().forServers().forRemotes().nodes().size() > 0) {
                    externalNode = ignite.cluster().forServers().forRemotes().forRandom().node().id();
                    replicateData();
                    metaCache.put(localNode + delimiter + "replicated", externalNode.toString());
                    state = 1;
                    metaCache.remove(localNode + delimiter + "local");
                    System.out.println("Replication successful on node " + externalNode);
                }else{
                    System.out.println("Could not find a node for replication! Data stay local only!");
                }
            }else if (cpuload >= 0.25){ //Mild load, partition data
                System.out.println("Partitioning needed");
                if(ignite.cluster().forServers().forRemotes().nodes().size() > 0) {
                    externalNode = ignite.cluster().forServers().forRemotes().forRandom().node().id();
                    partitionData();
                    metaCache.put(localNode + delimiter + "partitioned", externalNode.toString() + delimiter + localFilter.replaceFirst("local" + delimiter, ""));
                    state = 2;
                    metaCache.remove(localNode + delimiter + "local");
                    System.out.println("Partitioning successful on node " + externalNode);
                }else{
                    System.out.println("Could not find a node for partitioning! Data stay local only!");
                }
            }
        }
        return null;
    }

    private void partitionData(){

        System.out.println("Partitioning data from " + localNode + " to " + externalNode);
        //TODO Create the filter
        if(localFilter == null) localFilter = "local" + delimiter + "application";
        IgniteBiPredicate<String, Object> filter = (key, val) -> key.startsWith(localFilter);
        //Get local data
        ExtractionService extractionService = ignite.services(ignite.cluster().forLocal()).serviceProxy(ExtractionInterface.SERVICE_NAME,
                ExtractionInterface.class, false);
        HashMap<String, Object> localData = extractionService.extractData(filter, true);
        //Generate remote keys
        HashMap<String,Object> data = new HashMap<>();
        localData.forEach((k,v) -> {
            String newKey = k.replaceFirst("local", localNode.toString());
            data.put(newKey, v);
        });
        //Write them to remote node
        IngestionInterface ingestionInterface = ignite.services(ignite.cluster().forNodeId(externalNode)).serviceProxy(IngestionInterface.SERVICE_NAME,
                IngestionInterface.class, false);
        ingestionInterface.ingestData(data);


    }

    private void replicateData(){

        System.out.println("Replicating data from " + localNode + " to " + externalNode);
        //Get all local data
        ExtractionService extractionService = ignite.services(ignite.cluster().forLocal()).serviceProxy(ExtractionInterface.SERVICE_NAME,
                ExtractionInterface.class, false);
        //Create filter
        IgniteBiPredicate<String, Object> filter = (key, val) -> key.contains("local" + delimiter);
        //Get local data
        HashMap<String, Object> localData = extractionService.extractData(filter, false);
        //Generate remote keys
        HashMap<String,Object> data = new HashMap<>();
        localData.forEach((k,v) -> {
            String newKey = k.replaceFirst("local", localNode.toString());
            data.put(newKey, v);
        });
        //Write them to remote node
        IngestionInterface ingestionInterface = ignite.services(ignite.cluster().forNodeId(externalNode)).serviceProxy(IngestionInterface.SERVICE_NAME,
                IngestionInterface.class, false);
        ingestionInterface.ingestData(data);


    }*/

}
