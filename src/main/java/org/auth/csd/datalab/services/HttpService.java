package org.auth.csd.datalab.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.ServerNodeStartup;
import org.auth.csd.datalab.common.interfaces.DataManagementInterface;
import org.auth.csd.datalab.common.interfaces.HttpInterface;
import org.auth.csd.datalab.common.interfaces.MovementInterface;
import org.auth.csd.datalab.common.models.InputJson;
import org.auth.csd.datalab.common.models.Message;
import org.auth.csd.datalab.common.models.Monitoring;
import org.auth.csd.datalab.common.models.keys.AnalyticKey;
import org.auth.csd.datalab.common.models.keys.MetricKey;
import org.auth.csd.datalab.common.models.values.MetaMetric;
import org.auth.csd.datalab.common.models.values.Metric;
import org.auth.csd.datalab.common.models.values.TimedMetric;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rapidoid.buffer.Buf;
import org.rapidoid.http.AbstractHttpServer;
import org.rapidoid.http.HttpStatus;
import org.rapidoid.http.HttpUtils;
import org.rapidoid.net.Server;
import org.rapidoid.net.abstracts.Channel;
import org.rapidoid.net.impl.RapidoidHelper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class HttpService implements HttpInterface {

    @IgniteInstanceResource
    private Ignite ignite;
    private static HashMap<String, Integer> aggregations = new HashMap<String, Integer>() {
        {
            put("max", 0);
            put("min", 1);
            put("sum", 2);
            put("avg", 3);
        }
    };

    private Server server;

    /**
     * {@inheritDoc}
     */
    public void init(ServiceContext ctx) {
        server = new CustomHttpServer().listen(50000);
        System.out.println("Initializing Http Server on node:" + ignite.cluster().localNode());
    }

    /**
     * {@inheritDoc}
     */
    public void execute(ServiceContext ctx) {
        System.out.println("Executing Http Server on node:" + ignite.cluster().localNode());
    }

    /**
     * {@inheritDoc}
     */
    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Http Server on node:" + ignite.cluster().localNode());
        server.shutdown();
    }

    //------------Common----------------
    //Get filters from json
    private HashMap<String, HashSet<String>> getFilters(JSONObject obj) {
        HashMap<String, HashSet<String>> filters = new HashMap<>();
        if (obj.has("metricID"))
            filters.put("metricID", new HashSet<>(obj.getJSONArray("metricID").toList().stream().map(Object::toString).collect(Collectors.toList())));
        if (obj.has("entityID"))
            filters.put("entityID", new HashSet<>(obj.getJSONArray("entityID").toList().stream().map(Object::toString).collect(Collectors.toList())));
        if (obj.has("podName"))
            filters.put("podName", new HashSet<>(obj.getJSONArray("podName").toList().stream().map(Object::toString).collect(Collectors.toList())));
        if (obj.has("podNamespace"))
            filters.put("podNamespace", new HashSet<>(obj.getJSONArray("podNamespace").toList().stream().map(Object::toString).collect(Collectors.toList())));
        if (obj.has("containerName"))
            filters.put("containerName", new HashSet<>(obj.getJSONArray("containerName").toList().stream().map(Object::toString).collect(Collectors.toList())));
        return filters;
    }

    //Get nodes (hostnames) from json
    private HashSet<String> getNodes(JSONObject obj) {
        HashSet<String> nodesList = new HashSet<>();
        if (obj.has("nodes")) { //Get data from the cluster
            JSONArray nodes = obj.getJSONArray("nodes");
            nodesList = nodes.toList().stream().map(Object::toString).collect(Collectors.toCollection(HashSet::new));
        }
        return nodesList;
    }

    //Get first timestamp from json
    private long getFrom(JSONObject obj) {
        if (obj.has("from") && obj.getLong("from") >= 0) {
            return obj.getLong("from");
        } else return -1;
    }

    //Get second timestamp from json
    private long getTo(JSONObject obj, long from) {
        if (obj.has("to") && obj.getLong("to") >= from) {
            return obj.getLong("to");
        } else return -1;
    }

    //Get filters from json
    private HashSet<String> getKeys(JSONObject obj) {
        HashSet<String> keys = new HashSet<>();
        if (obj.has("key")) {
            JSONArray tmpKeys = obj.getJSONArray("key");
            keys.addAll(tmpKeys.toList().stream().map(Object::toString).collect(Collectors.toList()));
        }
        return keys;
    }

    //------------ANALYTICS----------------
    private HashMap<AnalyticKey, Metric> extractAnalyticsFromJson(JSONArray obj) throws IOException {
        HashMap<AnalyticKey, Metric> data = new HashMap<>();
        for (int i = 0; i < obj.length(); i++) {
            JSONObject o = obj.getJSONObject(i);
            if (o.has("key") && o.has("val") && o.has("timestamp")) {
                data.put(new AnalyticKey(o.getString("key"), o.getLong("timestamp")), new Metric(o.getDouble("val")));
            }
        }
        return data;
    }

    private String beautifyAnalytics(String key, List<TimedMetric> values) {
        StringBuilder result = new StringBuilder("{\"key\": \"" + key + "\",");
        result.append("\"values\": [");
        if (!values.isEmpty()) {
            values.forEach(v -> result.append("{").append(v).append("},"));
            result.deleteCharAt(result.length() - 1);
        }
        result.append("]}");
        return result.toString();
    }

    //------------MONITORING----------------
    private HashMap<MetricKey, InputJson> extractMonitoringFromJson(JSONArray obj) throws IOException {
        HashMap<MetricKey, InputJson> metrics = new HashMap<>();
        for (int i = 0; i < obj.length(); i++) {
            JSONObject o = obj.getJSONObject(i);
            ObjectMapper m = new ObjectMapper();
            try {
                InputJson myMetric = m.readValue(o.toString(), InputJson.class);
                metrics.put(new MetricKey(myMetric.metricID, myMetric.entityID), myMetric);
            } catch (IOException e) {
                throw e;
            }
        }
        return metrics;
    }

    private String beautifyMonitoring(MetricKey key, MetaMetric metadata, List<TimedMetric> values) {
        StringBuilder result = new StringBuilder("{" + key + ",");
        result.append("\"values\": [");
        if (!values.isEmpty()) {
            values.forEach(v -> result.append("{").append(v).append("},"));
            result.deleteCharAt(result.length() - 1);
        }
        result.append("],").append(metadata).append("}");
        return result.toString();
    }

    //------------SERVER----------------
    private class CustomHttpServer extends AbstractHttpServer {

        //Requests
        private final byte[] REQ_POST = "POST".getBytes();
        private final byte[] REQ_GET = "GET".getBytes();
        private final byte[] REQ_DEL = "DELETE".getBytes();
        //Helpers
        private final byte[] URI_NODES = "/nodes".getBytes();
        //Monitoring
        private final byte[] URI_MON = "/monitoring".getBytes();
        private final byte[] URI_MON_PUT = "/put".getBytes();
        private final byte[] URI_MON_GET = "/get".getBytes();
        private final byte[] URI_MON_QUERY = "/query".getBytes();
        private final byte[] URI_MON_LIST = "/list".getBytes();
        //Analytics cache
        private final byte[] URI_ANALYTICS = "/analytics".getBytes();
        private final byte[] URI_ANALYTICS_PUT = "/analytics/put".getBytes();
        private final byte[] URI_ANALYTICS_GET = "/analytics/get".getBytes();
        //App cache
        private final byte[] URI_APP = "/app".getBytes();
        private final byte[] URI_APP_PUT = "/app/put".getBytes();
        private final byte[] URI_APP_GET = "/app/get".getBytes();

        @Override
        protected HttpStatus handle(Channel ctx, Buf buf, RapidoidHelper req) {
            if (matches(buf, req.verb, REQ_POST)) {
                //PUT monitoring data
                if (matches(buf, req.path, URI_MON_PUT)) {
                    //Read and parse json
                    try {
                        HashMap<MetricKey, InputJson> metrics;
                        String body = buf.get(req.body);
                        JSONObject obj = new JSONObject(body);
                        //Get the monitoring keyword and parse the data
                        JSONArray monitor = obj.getJSONArray("monitoring");
                        try {
                            metrics = extractMonitoringFromJson(monitor);
                        } catch (IOException e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on json schema!"));
                        }
                        MovementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
                        srvInterface.ingestMonitoring(metrics);
                        return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("OK", "Ingestion successful!"));
                    } catch (Exception e) {
                        e.printStackTrace();
                        return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data ingestion!"));
                    }
                }
                //GET monitoring data
                else if (matches(buf, req.path, URI_MON_GET)) {
                    StringBuilder result = new StringBuilder("{\"monitoring\": [");
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            //Get time for historical or latest if they are missing
                            long from = getFrom(obj);
                            long to = getTo(obj, from);
                            //Get filters
                            HashMap<String, HashSet<String>> filters = getFilters(obj);
                            //Get nodes
                            HashSet<String> nodesList = getNodes(obj);
                            //Send stuff to movement service
                            MovementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
                            HashMap<String, HashMap<MetricKey, Monitoring>> data = srvInterface.extractMonitoring(filters, from, to, nodesList);
                            for (String node : data.keySet()) {
                                result.append("{\"node\": \"").append(node).append("\", \"data\": [");
                                if (!data.get(node).isEmpty()) {
                                    data.get(node).forEach((k, v) -> result.append(beautifyMonitoring(k, v.metadata, v.values)).append(","));
                                    result.deleteCharAt(result.length() - 1);
                                }
                                result.append("]}");
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    result.append("]}");
                    return json(ctx, req.isKeepAlive.value, result.toString().getBytes());
                }
                //GET monitoring aggregation
                else if (matches(buf, req.path, URI_MON_QUERY)) {
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            //Get time for historical or latest if they are missing
                            long from = getFrom(obj);
                            long to = getTo(obj, from);
                            //Get filters
                            HashMap<String, HashSet<String>> filters = getFilters(obj);
                            //Get nodes
                            HashSet<String> nodesList = getNodes(obj);
                            //Get aggregations
                            int aggreg = (obj.has("agg") && aggregations.containsKey(obj.getString("agg"))) ? aggregations.get(obj.getString("agg")) : -1;
                            //Send stuff to movement service
                            MovementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
                            if (aggreg >= 0 && aggreg < 4) {
                                Double data = srvInterface.extractMonitoringSingle(filters, from, to, nodesList, aggreg);
                                return json(ctx, req.isKeepAlive.value, ("{\"value\": " + data + "}").getBytes());
                            } else
                                return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Wrong aggregation!"));
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                }
                //get LIST of monitoring metrics
                else if (matches(buf, req.path, URI_MON_LIST)) {
                    StringBuilder result = new StringBuilder("{\"metric\": [");
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            //Get filters
                            HashMap<String, HashSet<String>> filters = getFilters(obj);
                            //Get return fields
                            HashSet<String> returns = new HashSet<>();
                            if (obj.has("fields")) { //Get data from the cluster
                                JSONArray nodes = obj.getJSONArray("fields");
                                returns.addAll(nodes.toList().stream().map(Object::toString).collect(Collectors.toCollection(HashSet::new)));
                            }
                            //Get Data
                            MovementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
                            HashMap<MetricKey, MetaMetric> metricData = srvInterface.extractMonitoringList(filters);
                            if (!metricData.isEmpty()) {
                                metricData.forEach((k, v) -> result.append("{").append(k).append(",").append((returns.isEmpty()) ? v : v.toString(returns)).append("},"));
                                result.deleteCharAt(result.length() - 1);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    result.append("]}");
                    return json(ctx, req.isKeepAlive.value, result.toString().getBytes());
                }
                //PUT analytics data
                else if (matches(buf, req.path, URI_ANALYTICS_PUT)) {
                    //Read and parse json
                    try {
                        HashMap<AnalyticKey, Metric> data;
                        String body = buf.get(req.body);
                        JSONObject obj = new JSONObject(body);
                        //Get the analytics keyword and parse the data
                        JSONArray analytics = obj.getJSONArray("analytics");
                        try {
                            data = extractAnalyticsFromJson(analytics);
                        } catch (IOException e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on json schema!"));
                        }
                        DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
                        srvInterface.ingestAnalytics(data);
                        return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("OK", "Ingestion successful!"));
                    } catch (Exception e) {
                        e.printStackTrace();
                        return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data ingestion!"));
                    }
                }
                //GET analytics data
                else if (matches(buf, req.path, URI_ANALYTICS_GET)) {
                    StringBuilder result = new StringBuilder("{\"analytics\": [");
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            //Get set of analytics keys
                            HashSet<String> ids = getKeys(obj);
                            //Get time for historical or latest if they are missing
                            long from = getFrom(obj);
                            long to = getTo(obj, from);
                            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
                            HashMap<String, List<TimedMetric>> data = srvInterface.extractAnalytics(ids, from, to);
                            if (!data.isEmpty()) {
                                data.forEach((k, v) -> result.append(beautifyAnalytics(k, v)).append(","));
                                result.deleteCharAt(result.length() - 1);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    result.append("]}");
                    return json(ctx, req.isKeepAlive.value, result.toString().getBytes());
                }
                //PUT application data (SIMILAR TO ANALYTICS)
                else if (ServerNodeStartup.app_cache && matches(buf, req.path, URI_APP_PUT)) {
                    //Read and parse json
                    try {
                        HashMap<AnalyticKey, Metric> data;
                        String body = buf.get(req.body);
                        JSONObject obj = new JSONObject(body);
                        //Get the application keyword and parse the data
                        JSONArray analytics = obj.getJSONArray("application");
                        try {
                            //Similar to analytics data (key, value, timestamp)
                            data = extractAnalyticsFromJson(analytics);
                        } catch (IOException e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on json schema!"));
                        }
                        DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
                        srvInterface.ingestApp(data);
                        return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("OK", "Ingestion successful!"));
                    } catch (Exception e) {
                        e.printStackTrace();
                        return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data ingestion!"));
                    }
                }
                //GET application data (SIMILAR TO ANALYTICS)
                else if (ServerNodeStartup.app_cache && matches(buf, req.path, URI_APP_GET)) {
                    StringBuilder result = new StringBuilder("{\"application\": [");
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            //Get set of analytics keys
                            HashSet<String> ids = getKeys(obj);
                            //Get time for historical or latest if they are missing
                            long from = getFrom(obj);
                            long to = getTo(obj, from);
                            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
                            HashMap<String, List<TimedMetric>> data = srvInterface.extractApp(ids, from, to);
                            if (!data.isEmpty()) {
                                data.forEach((k, v) -> result.append(beautifyAnalytics(k, v)).append(","));
                                result.deleteCharAt(result.length() - 1);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    result.append("]}");
                    return json(ctx, req.isKeepAlive.value, result.toString().getBytes());
                }
                //GET list of nodes
                else if (matches(buf, req.path, URI_NODES)) {
                    StringBuilder result = new StringBuilder("{\"nodes\": [");
                    MovementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(MovementInterface.SERVICE_NAME, MovementInterface.class, false);
                    HashMap<String, Boolean> nodes = srvInterface.extractNodes();
                    if (!nodes.isEmpty()) {
                        nodes.forEach((k, v) -> result
                                .append("{\"hostname\": \"")
                                .append(k)
                                .append("\",")
                                .append("\"cluster_head\": ")
                                .append(v)
                                .append("},"));
                        result.deleteCharAt(result.length() - 1);
                    }
                    result.append("]}");
                    return json(ctx, req.isKeepAlive.value, result.toString().getBytes());
                }
            } else if (matches(buf, req.verb, REQ_DEL)) {
                //Delete monitoring
                if (matches(buf, req.path, URI_MON)) {
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            HashMap<String, HashSet<String>> filters = getFilters(obj);
                            if (filters.isEmpty())
                                return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Empty keyset!"));
                            //Delete stuff
                            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
                            if (srvInterface.deleteMonigoring(filters))
                                return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("OK", "Delete successful!"));
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error during deletion!"));
                        }
                    }
                    return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error during deletion!"));
                }
                //Delete analytics
                else if (matches(buf, req.path, URI_ANALYTICS)) {
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            HashSet<String> ids = getKeys(obj);
                            if (ids.isEmpty())
                                return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Empty keyset!"));
                            //Delete stuff
                            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
                            if (srvInterface.deleteAnalytics(ids))
                                return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("OK", "Delete successful!"));
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error during deletion!"));
                        }
                    }
                    return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error during deletion!"));
                }
                //Delete application (SIMILAR TO ANALYTICS)
                else if (matches(buf, req.path, URI_APP)) {
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            HashSet<String> ids = getKeys(obj);
                            if (ids.isEmpty())
                                return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Empty keyset!"));
                            //Delete stuff
                            DataManagementInterface srvInterface = ignite.services(ignite.cluster().forLocal()).serviceProxy(DataManagementInterface.SERVICE_NAME, DataManagementInterface.class, false);
                            if (srvInterface.deleteApp(ids))
                                return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("OK", "Delete successful!"));
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error during deletion!"));
                        }
                    }
                    return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error during deletion!"));
                }
            }
            return HttpStatus.NOT_FOUND;
        }
    }

}
