package org.auth.csd.datalab;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.ClientExtractionInterface;
import org.auth.csd.datalab.common.ExtractionInterface;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class ClientExtractionService implements ClientExtractionInterface {

    //TODO ENV variables
    private String cacheName = "Metadata";
    @IgniteInstanceResource
    private Ignite ignite;
    /** Reference to the cache. */
    private IgniteCache<String, String> metaCache;
    private static String splitdelimiter = "\\.";
    private static String delimiter = ".";
    /** Processor that accepts requests from external apps that don't use Apache Ignite API. */
    private ExternalCallsProcessor externalCallsProcessor;

    /** {@inheritDoc} */
    public void init(ServiceContext ctx) throws Exception {
        System.out.println("Initializing Extraction Service on node:" + ignite.cluster().localNode());
        /**
         * It's assumed that the cache has already been deployed. To do that, make sure to start Data Nodes with
         * a respective cache configuration.
         */
        metaCache = ignite.cache(cacheName);
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

    private HashSet<String> jsonToSet(JSONObject obj, String key){
        HashSet<String> keys = new HashSet<>();
        JSONArray meta = obj.getJSONArray(key);
        for (int i = 0; i< meta.length(); i++){
            String data = meta.getString(i);
            keys.add(data);
        }
        return keys;
    }

    private HashSet<String> jsonToSetRecursive(JSONObject obj, String key){
        HashSet<String> keys = new HashSet<>();
        JSONObject monitor = obj.getJSONObject(key);
        //TODO hardcoded two level height
        for(String firstKeys : monitor.keySet()){
            //Create key hash
            JSONArray tmp = monitor.getJSONArray(firstKeys);
            for (int i = 0; i< tmp.length(); i++){
                String data = tmp.getString(i);
                String cacheKey = firstKeys + delimiter + data;
                keys.add(cacheKey);
            }
        }
        return keys;
    }

    private HashMap<String, HashSet<String>> getNodeQueries(HashSet<String> nodes, Set<String> data){
        //Get the meta data
        HashMap<String, Object> metadata = extractMeta(nodes);
        //Find the nodes and data to query for each node
        HashMap<String, HashSet<String>> keysWNodes = new HashMap<>();
        metadata.forEach((k,v) -> {
            if(v.getClass() == String.class && v.toString().equals("local")){ //Then it is local
                HashSet<String> tmpData = keysWNodes.getOrDefault(k, new HashSet<>());
                data.forEach(entry -> tmpData.add(k + delimiter + entry));
                keysWNodes.put(k, tmpData);
            }else if(v.getClass() == HashMap.class){
                //Tmp middle hash
                HashMap<String, Object> tmpHash = (HashMap<String, Object>) v;
                //If the node is replicated check which one best suits the transfer of data
                if(tmpHash.containsKey("replicated")){
                    HashSet<String> replicas = (HashSet<String>) tmpHash.get("replicated");
                    String res = k;
                    for(String entry: replicas) {
                        if (keysWNodes.containsKey(entry)){
                            res = entry;
                            break;
                        }
                    }
                    HashSet<String> tmpData = keysWNodes.getOrDefault(res, new HashSet<>());
                    data.forEach(entry -> tmpData.add(k + delimiter + entry));
                    keysWNodes.put(res, tmpData);

                }
                //If the node is partitioned get the data from each partition based on the filter
                else if(tmpHash.containsKey("partitioned")){
                    HashMap<String, HashSet<String>> partitions = (HashMap<String, HashSet<String>>) tmpHash.get("partitioned");
                    for(String dataKey : data){
                        boolean found = false;
                        for(String partition: partitions.keySet()){
                            for (String filterTmp: partitions.get(partition)){
                                if(dataKey.startsWith(filterTmp)) {
                                    found = true;
                                    break;
                                }
                            }
                            if(found){
                                HashSet<String> tmpData = keysWNodes.getOrDefault(partition, new HashSet<>());
                                tmpData.add(k + delimiter + dataKey);
                                keysWNodes.put(partition, tmpData);
                                break;
                            }
                        }
                        if(!found){
                            HashSet<String> tmpData = keysWNodes.getOrDefault(k, new HashSet<>());
                            tmpData.add(k + delimiter + dataKey);
                            keysWNodes.put(k, tmpData);
                        }
                    }
                }
            }
        });
        return keysWNodes;
    }

    @Override
    public HashMap<String, Object> extractMeta(HashSet<String> queriedNodes) {
        HashMap<String, Object> data = new HashMap<>();
        metaCache.query(new ScanQuery<>(null)).forEach(entry -> {
            String k = (String) entry.getKey();
            String v = (String) entry.getValue();
            String[] splitKey = k.split(splitdelimiter);
            String source = splitKey[0];
            String type = splitKey[1];
            if(type.equals("partitioned")){
                HashMap<String, Object> tmp = new HashMap<>();
                HashMap<String, HashSet<String>> tmp2 = new HashMap<>();
                String target = v.split(splitdelimiter)[0];
                String tmpfilter = v.replace(target+delimiter, "");
                HashSet<String> filters = new HashSet<String>() {
                    {
                        add(tmpfilter);
                    }
                };
                tmp2.put(target, filters);
                tmp.put(type, tmp2);
                data.put(source, tmp);
            }else if (type.equals("replicated")){
                HashMap<String, Set<String>> tmp = new HashMap<>();
                Set<String> nodes = new HashSet<>();
                Collections.addAll(nodes, v.split(splitdelimiter));
                tmp.put(type, nodes);
                data.put(source,tmp);
            }else{
                data.put(source, type);
            }
        });
        return data;
    }

    @Override
    public HashMap<String, Object> extractData(HashMap<String, HashSet<String>> data) {
        //Ready map for data
        HashMap<String, Object> result = new HashMap<>();
        for(String node: data.keySet()){
            //Transform a little the set
            HashSet<String> tmpData = new HashSet<>();
            for(String entry: data.get(node)){
                if(entry.startsWith(node + delimiter)) tmpData.add(entry.replaceFirst(node, "local"));
                else tmpData.add(entry);
            }
            IgniteBiPredicate<String, Object> filter = (key, val) -> tmpData.contains(key);
            ExtractionInterface extractionInterface = ignite.services(ignite.cluster().forNodeId(UUID.fromString(node))).serviceProxy(ExtractionInterface.SERVICE_NAME,
                    ExtractionInterface.class, false);
            HashMap<String, Object> nodeData = extractionInterface.extractData(filter, false);
            result.put(node, nodeData);
        }
        return result;
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
                externalConnect = new ServerSocket(50002);
                while (!isInterrupted()) {
                    Socket socket = externalConnect.accept();
                    DataInputStream dis = new DataInputStream(socket.getInputStream());
                    //Prepare output key
                    JSONObject json = null;
                    //Get json keys
                    String jsonString = dis.readUTF();
                    JSONObject obj = new JSONObject(jsonString);
                    if(obj.has("metadata")) {
                        HashSet<String> keys = jsonToSet(obj, "metadata");
                        //Get data
                        HashMap<String, Object> data = extractMeta(keys);
                        //Create the returned json
                        json = new JSONObject();
                        json.put("metadata", data);
                    }
                    else if(obj.has("monitoring")) {
                        JSONObject mon = obj.getJSONObject("monitoring");
                        //Get the nodes for the extraction
                        HashSet<String> nodes = jsonToSet(mon, "nodes");
                        //Get the data list for the extraction
                        HashSet<String> monitoring = jsonToSetRecursive(mon, "data");
                        //Find the nodes that need to be queried
                        HashMap<String, HashSet<String>> queries = getNodeQueries(nodes, monitoring);
                        //Get data
                        HashMap<String, Object> result = extractData(queries);
                        //Create the returned json
                        json = new JSONObject();
                        json.put("monitoring", result);
                    }
                    //Return data
                    ObjectOutputStream dos = new ObjectOutputStream(socket.getOutputStream());
                    // Writing the result into the socket.
                    assert json != null;
                    dos.writeObject(json.toString());
                    //Close string
                    dos.close();
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
