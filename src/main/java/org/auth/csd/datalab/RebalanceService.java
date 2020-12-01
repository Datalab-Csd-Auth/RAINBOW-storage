package org.auth.csd.datalab;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.ExtractionInterface;
import org.auth.csd.datalab.common.IngestionInterface;
import org.auth.csd.datalab.common.RebalanceInterface;

import java.util.*;

public class RebalanceService implements RebalanceInterface {

    //TODO ENV variables
    private String monName = "Monitoring";
    private String metaName = "Metadata";
    @IgniteInstanceResource
    private Ignite ignite;
    /** Reference to the cache. */
    private IgniteCache<String, String> metaCache;
    private IgniteCache<String, Object> monCache;
    private String delimiter = ".";
    /**
     * TODO node can have 1 status."
     * status = 0, normal state
     * status = 1, replicated state
     * status = 2, partitioned state
     */
    private short state = 0;
    /**
     * Filter for partitioning
     */
    private String localFilter = null;
    /**
     * TODO can be replicated/partitioned to one node
     */
    UUID externalNode = null;
    UUID localNode = null;

    /** {@inheritDoc} */
    public void init(ServiceContext ctx) throws Exception {
        System.out.println("Initializing Rebalance Service on node:" + ignite.cluster().localNode());
        /**
         * It's assumed that the cache has already been deployed. To do that, make sure to start Data Nodes with
         * a respective cache configuration.
         */
        metaCache = ignite.cache(metaName);
        monCache = ignite.cache(monName);
        localNode = ignite.cluster().localNode().id();
        metaCache.put(localNode.toString() + delimiter + "local", localNode.toString());
    }

    /** {@inheritDoc} */
    public void execute(ServiceContext ctx) throws Exception {
        System.out.println("Executing Rebalance Service on node:" + ignite.cluster().localNode());
    }

    /** {@inheritDoc} */
    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Rebalance Service on node:" + ignite.cluster().localNode());
    }

    @Override
    public String rebalanceData(Set<String> keys) {
        //Start by checking if cache is replicated
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
                externalNode = ignite.cluster().forServers().forRemotes().forRandom().node().id();
                replicateData();
                metaCache.put(localNode + delimiter + "replicated", externalNode.toString());
                state = 1;
                metaCache.remove(localNode + delimiter + "local");
            }else if (cpuload >= 0.25){ //Mild load, partition data
                System.out.println("Partitioning needed");
                externalNode = ignite.cluster().forServers().forRemotes().forRandom().node().id();
                partitionData();
                metaCache.put(localNode + delimiter + "partitioned", externalNode.toString() + delimiter + localFilter.replaceFirst("local" + delimiter,""));
                state = 2;
                metaCache.remove(localNode + delimiter + "local");
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
    }

}
