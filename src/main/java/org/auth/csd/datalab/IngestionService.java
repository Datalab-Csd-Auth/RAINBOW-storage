package org.auth.csd.datalab;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.IngestionInterface;
import org.auth.csd.datalab.common.RebalanceInterface;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class IngestionService implements IngestionInterface {

    //TODO ENV variables
    private String cacheName = "Monitoring";
    @IgniteInstanceResource
    private Ignite ignite;
    /** Reference to the cache. */
    private IgniteCache<String, Object> myCache;
    private String delimiter = ".";
    /** Processor that accepts requests from external apps that don't use Apache Ignite API. */
    private ExternalCallsProcessor externalCallsProcessor;

    /** {@inheritDoc} */
    public void init(ServiceContext ctx) throws Exception {
        System.out.println("Initializing Ingestion Service on node:" + ignite.cluster().localNode());
        /**
         * It's assumed that the cache has already been deployed. To do that, make sure to start Data Nodes with
         * a respective cache configuration.
         */
        myCache = ignite.cache(cacheName);
        /** Processor that accepts requests from external apps that don't use Apache Ignite API. */
        externalCallsProcessor = new ExternalCallsProcessor();
        externalCallsProcessor.start();
    }

    /** {@inheritDoc} */
    public void execute(ServiceContext ctx) throws Exception {
        System.out.println("Executing Ingestion Service on node:" + ignite.cluster().localNode());
    }

    /** {@inheritDoc} */
    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Ingestion Service on node:" + ignite.cluster().localNode());
        // Stopping external requests processor.
        externalCallsProcessor.interrupt();
    }

    private HashMap<String, Object> jsonToMap(String jsonString){
        HashMap<String, Object> data = new HashMap<>();
        JSONObject obj = new JSONObject(jsonString);
        //Get the monitoring keyword
        JSONObject monitor = obj.getJSONObject("monitoring");
        //Get the data (2-lvl depth)
        for(String key : monitor.keySet()){
            JSONObject tmp = monitor.getJSONObject(key);
            for(String key2 : tmp.keySet()){
                Object val = tmp.get(key2);
                String inputKey = "local" + delimiter + key + delimiter + key2;
                data.put(inputKey, val);
            }
        }
        return data;
    }

    @Override
    public void ingestData(HashMap<String, Object> data) {
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            myCache.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Thread that accepts request from external applications that don't use Apache Ignite service grid API.
     */
    private class ExternalCallsProcessor extends Thread {

        /** Server socket to accept external connections. */
        private ServerSocket externalConnect;

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                externalConnect = new ServerSocket(50000);
                while (!isInterrupted()) {
                    //Input data structure
                    HashMap<String,Object> data = null;
                    //Read from the socket
                    Socket socket = externalConnect.accept();
                    DataInputStream dis = new DataInputStream(socket.getInputStream());
                    //Get ingestion data
                    try{
                        String jsonString = dis.readUTF();
                        //Parse json and save data
                        data = jsonToMap(jsonString);
                        ingestData(data);
                    }catch (Exception e){
                        System.out.println("Could not read input data! Ingestion failed!");
                    }
                    //Close input stream
                    dis.close();
                    socket.close();
                    //Check if there are input data
                    if(data != null && !data.isEmpty()) {
                        // Rebalance data on local node
                        RebalanceService rebalanceService = ignite.services(ignite.cluster().forLocal()).serviceProxy(RebalanceInterface.SERVICE_NAME,
                                RebalanceInterface.class, false);
                        rebalanceService.rebalanceData(data.keySet());
                    }
                }
            }
            catch (IOException e) {
                e.printStackTrace();
                System.out.println("Could not create socket connection on port 50000!");
            }
        }

        /** {@inheritDoc} */
        @Override public void interrupt() {
            super.interrupt();
            try {
                externalConnect.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
