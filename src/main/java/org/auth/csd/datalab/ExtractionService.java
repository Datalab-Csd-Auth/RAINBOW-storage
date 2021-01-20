package org.auth.csd.datalab;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.ExtractionInterface;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class ExtractionService implements ExtractionInterface {

    //TODO ENV variables
    private String cacheName = "Monitoring";
    @IgniteInstanceResource
    private Ignite ignite;
    /** Reference to the cache. */
    private IgniteCache<String, Object> myCache;
    /** Processor that accepts requests from external apps that don't use Apache Ignite API. */
    private ExternalCallsProcessor externalCallsProcessor;
    UUID localNode = null;
    private String delimiter = ".";

    /** {@inheritDoc} */
    public void init(ServiceContext ctx) throws Exception {
        System.out.println("Initializing Extraction Service on node:" + ignite.cluster().localNode());
        /**
         * It's assumed that the cache has already been deployed. To do that, make sure to start Data Nodes with
         * a respective cache configuration.
         */
        myCache = ignite.cache(cacheName);
        localNode = ignite.cluster().localNode().id();
        /** Processor that accepts requests from external apps that don't use Apache Ignite API. */
        externalCallsProcessor = new ExternalCallsProcessor();
        externalCallsProcessor.start();
    }

    /** {@inheritDoc} */
    public void execute(ServiceContext ctx) throws Exception {
        System.out.println("Executing Extraction Service on node:" + ignite.cluster().localNode());
    }

    /** {@inheritDoc} */
    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Extraction Service on node:" + ignite.cluster().localNode());
        // Stopping external requests processor.
        externalCallsProcessor.interrupt();
    }

    private boolean containsKey(String key, Set<String> keyset){
        for(String tmp: keyset){
            if(tmp.equals(key)) return true;
        }
        return false;
    }

    private Set<String> jsonKeysToSet(String jsonString){
        Set<String> keys = new HashSet<>();
        JSONObject obj = new JSONObject(jsonString);
        JSONObject monitor = obj.getJSONObject("monitoring");
        //Get the node id for the data
        String prefix = ((monitor.has("node") && !monitor.getString("node").equals(localNode.toString())) ? monitor.getString("node") : "local");
        for(String key : monitor.keySet()){
            if(key.equals("node")) continue;
            //Create key hash
            JSONArray tmp = monitor.getJSONArray(key);
            for (int i = 0; i< tmp.length(); i++){
                String data = tmp.getString(i);
                String cacheKey = prefix + delimiter + key + delimiter + data;
                keys.add(cacheKey);
            }
        }
        return keys;
    }

    @Override
    public HashMap<String, Object> extractData(IgniteBiPredicate<String, Object> filter, boolean remove) {
        HashMap<String, Object> data = new HashMap<>();
        myCache.query(new ScanQuery<>(filter)).forEach(entry -> {
            data.put(entry.getKey(), entry.getValue());
        });
        if(remove) data.keySet().forEach(entry -> myCache.remove(entry));
        return data;
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
                externalConnect = new ServerSocket(50001);
                while (!isInterrupted()) {
                    //Data structure
                    HashMap<String, Object> data = null;
                    Socket socket = externalConnect.accept();
                    DataInputStream dis = new DataInputStream(socket.getInputStream());
                    try{
                        //Get json keys
                        String jsonString = dis.readUTF();
                        Set<String> keys = jsonKeysToSet(jsonString);
                        //Create filter
                        IgniteBiPredicate<String, Object> filter = (key, val) -> containsKey(key, keys);
                        //Get data
                        data = extractData(filter, false);
                    }catch (Exception e){
                        System.out.println("Could not read input data! Extraction failed!");
                    }
                    if(data != null && !data.isEmpty()) {
                        //Transform to json
                        JSONObject json = new JSONObject();
                        json.put("monitoring", data);
                        //Return data
                        ObjectOutputStream dos = new ObjectOutputStream(socket.getOutputStream());
                        // Writing the result into the socket.
                        dos.writeObject(data);
                        dos.close();
                    }
                    //Close streams
                    dis.close();
                    socket.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
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
