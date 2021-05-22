package org.auth.csd.datalab.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.interfaces.ClientExtractionInterface;
import org.auth.csd.datalab.common.interfaces.DataInterface;
import org.auth.csd.datalab.common.models.Message;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rapidoid.buffer.Buf;
import org.rapidoid.http.AbstractHttpServer;
import org.rapidoid.http.HttpStatus;
import org.rapidoid.http.HttpUtils;
import org.rapidoid.net.Server;
import org.rapidoid.net.abstracts.Channel;
import org.rapidoid.net.impl.RapidoidHelper;

import java.util.*;
import java.util.stream.Collectors;

public class ClientExtractionService implements ClientExtractionInterface {

    @IgniteInstanceResource
    private Ignite ignite;

    UUID localNode = null;
    private static String splitdelimiter = "\\.";
    private static String delimiter = ".";
    private Server server;

    /** {@inheritDoc} */
    public void init(ServiceContext ctx) throws Exception {
        System.out.println("Initializing Extraction Service on client node:" + ignite.cluster().localNode());
        localNode = ignite.cluster().localNode().id();
        server = new CustomHttpServer().listen(50001);
    }

    /** {@inheritDoc} */
    public void execute(ServiceContext ctx) throws Exception {
        System.out.println("Executing Extraction Service on client node:" + ignite.cluster().localNode());
    }

    /** {@inheritDoc} */
    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Extraction Service on client node:" + ignite.cluster().localNode());
        server.shutdown();
    }

    //------------MONITORING----------------

    private HashMap<String, String> getData(HashSet<String> ids, boolean entity) {
        HashMap<String, String> metrics = new HashMap<>();
        ClusterGroup servers = ignite.cluster().forServers();
        for (ClusterNode server : servers.nodes()){
            DataInterface extractionInterface = ignite.services(ignite.cluster().forNodeId(server.id())).serviceProxy(DataInterface.SERVICE_NAME,
                    DataInterface.class, false);
            HashMap<String,String> nodeData = extractionInterface.extractMonitoring(ids, entity);
            if(!nodeData.isEmpty())
                metrics.put(server.id().toString(), "{\"node\": \"" + server.id().toString() + "\", \"data\": [ " + String.join(", ", nodeData.values()) + " ]} ");
        }
        return metrics;
    }

    private HashMap<String, String> getData(HashSet<String> ids, boolean entity, long from, long to) {
        HashMap<String, String> metrics = new HashMap<>();
        ClusterGroup servers = ignite.cluster().forServers();
        for (ClusterNode server : servers.nodes()){
            DataInterface extractionInterface = ignite.services(ignite.cluster().forNodeId(server.id())).serviceProxy(DataInterface.SERVICE_NAME,
                    DataInterface.class, false);
            HashMap<String,String> nodeData = extractionInterface.extractMonitoring(ids, entity, from, to);
            if(!nodeData.isEmpty())
                metrics.put(server.id().toString(), "{\"node\": \"" + server.id().toString() + "\", \"data\": [ " + String.join(", ", nodeData.values()) + " ]} ");
        }
        return metrics;
    }
    //------------API----------------

    private class CustomHttpServer extends AbstractHttpServer {

        private final byte[] URI_GET = "/get".getBytes();

        @Override
        protected HttpStatus handle(Channel ctx, Buf buf, RapidoidHelper req) {
            if (!req.isGet.value) {
                if (matches(buf, req.path, URI_GET)) {
                    HashMap<String, String> result = new HashMap<>();
                    HashMap<String, String> meta = new HashMap<>();
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            boolean entityFlag = false;
                            HashSet<String> ids = new HashSet<>();
                            if (obj.has("entityID")) {
                                JSONArray entities = obj.getJSONArray("entityID");
                                List<String> entitiesList = entities.toList().stream().map(Object::toString).collect(Collectors.toList());
                                ids.addAll(entitiesList);
                                entityFlag = true;
                            } else if (obj.has("metricID")) {
                                JSONArray entities = obj.getJSONArray("metricID");
                                for (Object ent : entities) { //Store metric ids to list
                                    ids.add(ent.toString());
                                }
                            }
                            if (obj.has("latest") && obj.getBoolean("latest")) { //Get only the latest data
                                result = getData(ids, entityFlag);
                            } else if (obj.has("from") && obj.has("to")) {
                                long from = obj.getLong("from");
                                long to = obj.getLong("to");
                                result = getData(ids, entityFlag, from, to);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    String finalRes = "{\"monitoring\": [ " + String.join(", ", result.values()) + " ]} ";
                    return json(ctx, req.isKeepAlive.value, finalRes.getBytes());
                }
            }
            return HttpStatus.NOT_FOUND;
        }
    }


}
