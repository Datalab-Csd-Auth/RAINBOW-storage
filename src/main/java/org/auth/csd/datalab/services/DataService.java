package org.auth.csd.datalab.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.models.*;
import org.auth.csd.datalab.common.interfaces.DataInterface;
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

import static org.auth.csd.datalab.ServerNodeStartup.*;
import static org.auth.csd.datalab.common.Helpers.getNodesByHostnames;

public class DataService implements DataInterface {

    @IgniteInstanceResource
    private Ignite ignite;
    /**
     * Reference to the cache.
     */
    private IgniteCache<MetricKey, TimedMetric> myLatest;
    private IgniteCache<MetricTimeKey, Metric> myHistorical;
    private IgniteCache<MetricKey, MetaMetric> myMeta;
    private IgniteCache<String, TimedMetric> myAnalytics;
    private IgniteCache<String, String> myApp = null;

    UUID localNode = null;
    private String delimiter = ".";
    private Server server;

    /**
     * {@inheritDoc}
     */
    public void init(ServiceContext ctx) {
        System.out.println("Initializing Data Service on node:" + ignite.cluster().localNode());
        //Get the cache that is designed in the config for the latest data
        myLatest = ignite.cache(latestCacheName);
        myHistorical = ignite.cache(historicalCacheName);
        myMeta = ignite.cache(metaCacheName);
        myAnalytics = ignite.cache(analyticsCacheName);
        if (app_cache) myApp = ignite.cache(appCacheName);
        localNode = ignite.cluster().localNode().id();
        server = new CustomHttpServer().listen(50000);
    }

    /**
     * {@inheritDoc}
     */
    public void execute(ServiceContext ctx) {
        System.out.println("Executing Data Service on node:" + ignite.cluster().localNode());
    }

    /**
     * {@inheritDoc}
     */
    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Data Service on node:" + ignite.cluster().localNode());
        server.shutdown();
    }

    //------------APP cache----------------
    private void ingestAppData(HashMap<String, String> data) {
        for (Map.Entry<String, String> entry : data.entrySet()) {
            myApp.put(entry.getKey(), entry.getValue());
        }
    }

    private HashMap<String, String> extractAppData(List<String> search) {
        HashMap<String, String> data = new HashMap<>();
        IgniteBiPredicate<String, String> filter;
        if (!search.isEmpty()) filter = (key, val) -> search.contains(key);
        else filter = null;
        myApp.query(new ScanQuery<>(filter)).forEach(entry -> data.put(entry.getKey(), entry.getValue()));
        return data;
    }

    //------------ANALYTICS----------------
    private void ingestAnalytics(HashMap<String, TimedMetric> data) {
        for (Map.Entry<String, TimedMetric> entry : data.entrySet()) {
            myAnalytics.put(entry.getKey(), entry.getValue());
        }
    }

    private HashMap<String, String> extractAnalyticsData(Set<String> search) {
        HashMap<String, String> data = new HashMap<>();
        IgniteBiPredicate<String, TimedMetric> filter;
        if (!search.isEmpty()) filter = (key, val) -> search.contains(key);
        else filter = null;
        myAnalytics.query(new ScanQuery<>(filter)).forEach(entry -> data.put(entry.getKey(), "\"val\": " + entry.getValue().val + " , \"timestamp\": " + entry.getValue().timestamp));
        return data;
    }

    //------------MONITORING----------------
    private void ingestMetric(HashMap<MetricKey, InputJson> data) {
        for (Map.Entry<MetricKey, InputJson> entry : data.entrySet()) {
            myMeta.put(entry.getKey(), new MetaMetric(entry.getValue()));
            myLatest.put(entry.getKey(), new TimedMetric(entry.getValue().val, entry.getValue().timestamp));
            myHistorical.put(new MetricTimeKey(entry.getValue()), new Metric(entry.getValue().val));
        }
    }

    private String extractLatestData(MetricKey myKey) {
        List<String> res = new ArrayList<>();
        SqlFieldsQuery sql = new SqlFieldsQuery("SELECT timestamp, val FROM TIMEDMETRIC WHERE metricID = '" + myKey.metricID + "' AND entityID = '" + myKey.entityID + "'");
        return getString(res, myLatest.query(sql));
    }

    private String extractHistoricalData(MetricKey myKey, Long min, Long max) {
        List<String> res = new ArrayList<>();
        SqlFieldsQuery sql = new SqlFieldsQuery("SELECT timestamp, val FROM METRIC WHERE metricID = '" + myKey.metricID + "' AND entityID = '" + myKey.entityID + "' AND timestamp >= " + min + " AND timestamp <= " + max);
        return getString(res, myHistorical.query(sql));
    }

    private String getString(List<String> res, FieldsQueryCursor<List<?>> query) {
        try (QueryCursor<List<?>> cursor = query) {
            for (List<?> row : cursor) {
                Long time = (Long) row.get(0);
                Double value = (Double) row.get(1);
                res.add(" { \"timestamp\": " + time + " , \"val\": " + value + " } ");
            }
        }
        return " \"values\": [ " + String.join(", ", res) + " ] ";
    }

    private HashMap<MetricKey, String> extractMeta(HashMap<String, HashSet<String>> ids) {
        String select = " SELECT metricID, entityID, entityType, name, units, desc, groupName, minVal, maxVal, higherIsBetter, podUUID, podName, podNamespace, containerID, containerName ";
        String from = " FROM METAMETRIC ";
        List<String> where = new ArrayList<>();
        HashMap<MetricKey, String> meta = new HashMap<>();
        if (!ids.isEmpty()) {
            for (Map.Entry<String, HashSet<String>> entry : ids.entrySet()) {
                List<String> tmpWhere = new ArrayList<>();
                List<String> wildcard = entry.getValue().stream().filter(el -> el.endsWith("%")).collect(Collectors.toList());
                List<String> exact = new ArrayList<>(entry.getValue());
                exact.removeAll(wildcard);
                if(!exact.isEmpty()) tmpWhere.add(entry.getKey() + " IN ('" + String.join("','", entry.getValue()) + "') ");
                for (String key : wildcard){
                    tmpWhere.add(entry.getKey() + " LIKE '" + key + "' ");
                }
                where.add("(" + String.join(") OR (", tmpWhere) + ")");
            }
        }
        String finalWhere = (!where.isEmpty()) ? " WHERE (" + String.join(") AND (", where) + ") " : "";
        SqlFieldsQuery sql = new SqlFieldsQuery(select + from + finalWhere);
        try (QueryCursor<List<?>> cursor = myMeta.query(sql)) {
            for (List<?> row : cursor) {
                MetricKey myKey = new MetricKey(row.get(0).toString(), row.get(1).toString());
                String res = " \"entityType\": \"" + row.get(2) + "\"" +
                        ", \"name\": \"" + row.get(3) + "\"" +
                        ", \"units\": \"" + row.get(4) + "\"" +
                        ", \"desc\": \"" + row.get(5) + "\"" +
                        ", \"group\": \"" + row.get(6) + "\"" +
                        ", \"minVal\": " + row.get(7) +
                        ", \"maxVal\": " + row.get(8) +
                        ", \"higherIsBetter\": " + row.get(9) +
                        ", \"pod\": {" +
                        " \"uuid\": \"" + row.get(10) + "\"" +
                        ", \"name\": \"" + row.get(11) + "\"" +
                        ", \"namespace\": \"" + row.get(12) + "\"" +
                        "}" +
                        ", \"container\": {" +
                        " \"id\": \"" + row.get(13) + "\"" +
                        ", \"name\": \"" + row.get(14) + "\"" +
                        "} ";
                meta.put(myKey, res);
            }
        }
        return meta;
    }

    private HashSet<String> getMetricID() {
        HashSet<String> metrics = new HashSet<>();
        SqlFieldsQuery sql = new SqlFieldsQuery("select metricID from METAMETRIC");
        try (QueryCursor<List<?>> cursor = myMeta.query(sql)) {
            for (List<?> row : cursor)
                metrics.add(row.get(0).toString());
        }
        return metrics;
    }

    private HashSet<String> getMetricID(List<String> entities) {
        HashSet<String> metrics = new HashSet<>();
        for (String entity : entities) {
            SqlFieldsQuery sql = new SqlFieldsQuery("select metricID from METAMETRIC WHERE entityID = '" + entity + "'");
            try (QueryCursor<List<?>> cursor = myMeta.query(sql)) {
                for (List<?> row : cursor)
                    metrics.add(row.get(0).toString());
            }
        }
        return metrics;
    }

    //------------API----------------
    private class CustomHttpServer extends AbstractHttpServer {

        private final byte[] URI_PUT = "/put".getBytes();
        private final byte[] URI_GET = "/get".getBytes();
        //Analytics cache
        private final byte[] URI_ANALYTICS_PUT = "/analytics/put".getBytes();
        private final byte[] URI_ANALYTICS_GET = "/analytics/get".getBytes();
        //App cache
        private final byte[] URI_APP_PUT = "/app/put".getBytes();
        private final byte[] URI_APP_GET = "/app/get".getBytes();

        @Override
        protected HttpStatus handle(Channel ctx, Buf buf, RapidoidHelper req) {
            if (!req.isGet.value) {
                //PUT monitoring data
                if (matches(buf, req.path, URI_PUT)) {
                    //Data structure for metrics
                    HashMap<MetricKey, InputJson> metrics = new HashMap<>();
                    //Read and parse json
                    try {
                        String body = buf.get(req.body);
                        JSONObject obj = new JSONObject(body);
                        //Get the monitoring keyword and parse the data
                        JSONArray monitor = obj.getJSONArray("monitoring");
                        for (int i = 0; i < monitor.length(); i++) {
                            JSONObject o = monitor.getJSONObject(i);
                            ObjectMapper m = new ObjectMapper();
                            try {
                                InputJson myMetric = m.readValue(o.toString(), InputJson.class);
                                metrics.put(new MetricKey(myMetric.metricID, myMetric.entityID), myMetric);
                            } catch (IOException e) {
                                e.printStackTrace();
                                return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on json schema!"));
                            }
                        }
                        ingestMetric(metrics);
                        return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("OK", "Ingestion successful!"));
                    } catch (Exception e) {
                        e.printStackTrace();
                        return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data ingestion!"));
                    }
                }
                //GET monitoring data
                else if (matches(buf, req.path, URI_GET)) {
                    ArrayList<String> result = new ArrayList<>();
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            //Get time for historical or latest if they are missing
                            long from = -1, to = -1;
                            if (obj.has("from") && obj.has("to") && obj.getLong("from") >= 0 && obj.getLong("to") >= obj.getLong("from")) {
                                from = obj.getLong("from");
                                to = obj.getLong("to");
                            }
                            //Get filters
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

                            if (obj.has("nodes")) { //Get data from the cluster
                                JSONArray nodes = obj.getJSONArray("nodes");
                                HashSet<String> nodesList = nodes.toList().stream().map(Object::toString).collect(Collectors.toCollection(HashSet::new));
                                ClusterGroup servers = (nodesList.isEmpty()) ? ignite.cluster().forServers() : ignite.cluster().forServers().forPredicate(getNodesByHostnames(nodesList));
                                for (ClusterNode server : servers.nodes()) {
                                    DataInterface extractionInterface = ignite.services(ignite.cluster().forNodeId(server.id())).serviceProxy(DataInterface.SERVICE_NAME,
                                            DataInterface.class, false);
                                    ArrayList<String> nodeData = (from > -1) ? extractionInterface.extractMonitoring(filters, from, to) : extractionInterface.extractMonitoring(filters);
                                    if (!nodeData.isEmpty())
                                        result.add("{\"node\": \"" + server.hostNames().toString() + "\", \"data\": [ " + String.join(", ", nodeData) + " ]} ");
                                }
                            } else { //Get local data
                                result = (from > -1) ? extractMonitoring(filters, from, to) : extractMonitoring(filters);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    String finalRes = "{\"monitoring\": [ " + String.join(", ", result) + " ]} ";
                    return json(ctx, req.isKeepAlive.value, finalRes.getBytes());
                }
                //PUT analytics data
                else if (matches(buf, req.path, URI_ANALYTICS_PUT)) {
                    HashMap<String, TimedMetric> data = new HashMap<>();
                    //Read and parse json
                    try {
                        String body = buf.get(req.body);
                        JSONObject obj = new JSONObject(body);
                        //Get the monitoring keyword and parse the data
                        JSONArray analytics = obj.getJSONArray("analytics");
                        for (int i = 0; i < analytics.length(); i++) {
                            JSONObject o = analytics.getJSONObject(i);
                            if (o.has("key") && o.has("val") && o.has("timestamp")) {
                                data.put(o.getString("key"), new TimedMetric(o.getDouble("val"), o.getLong("timestamp")));
                            }
                        }
                        ingestAnalytics(data);
                        return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("OK", "Ingestion successful!"));
                    } catch (Exception e) {
                        e.printStackTrace();
                        return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data ingestion!"));
                    }
                }
                //GET analytics data
                else if (matches(buf, req.path, URI_ANALYTICS_GET)) {
                    HashMap<String, String> result = new HashMap<>();
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            HashSet<String> ids = new HashSet<>();
                            if (obj.has("key")) {
                                JSONArray tmpKeys = obj.getJSONArray("key");
                                ids.addAll(tmpKeys.toList().stream().map(Object::toString).collect(Collectors.toList()));
                            }
                            if (obj.has("nodes")) { //Get data from the cluster
                                JSONArray nodes = obj.getJSONArray("nodes");
                                HashSet<String> nodesList = nodes.toList().stream().map(Object::toString).collect(Collectors.toCollection(HashSet::new));
                                ClusterGroup servers = (nodesList.isEmpty()) ? ignite.cluster().forServers() : ignite.cluster().forServers().forPredicate(getNodesByHostnames(nodesList));
                                for (ClusterNode server : servers.nodes()) {
                                    DataInterface extractionInterface = ignite.services(ignite.cluster().forNodeId(server.id())).serviceProxy(DataInterface.SERVICE_NAME,
                                            DataInterface.class, false);
                                    HashMap<String, String> nodeData = extractionInterface.extractAnalytics(ids);
                                    if (!nodeData.isEmpty())
                                        result.put(server.id().toString(), "{\"node\": \"" + server.hostNames().toString() + "\", \"data\": [ " + String.join(", ", nodeData.values()) + " ]} ");
                                }
                            } else { //Get local data
                                result = extractAnalytics(ids);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    String finalRes = "{\"analytics\": [ " + String.join(", ", result.values()) + " ]} ";
                    return json(ctx, req.isKeepAlive.value, finalRes.getBytes());
                }
                //PUT application data
                else if (myApp != null && matches(buf, req.path, URI_APP_PUT)) {
                    //Data structure for app data
                    HashMap<String, String> data = new HashMap<>();
                    //Read and parse json
                    try {
                        String body = buf.get(req.body);
                        JSONObject obj = new JSONObject(body);
                        //Get the application keyword and parse the data
                        JSONArray application = obj.getJSONArray("application");
                        for (int i = 0; i < application.length(); i++) {
                            JSONObject o = application.getJSONObject(i);
                            if (o.has("key") && o.has("value")) {
                                data.put(o.getString("key"), o.getString("value"));
                            }
                        }
                        ingestAppData(data);
                        return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("OK", "Ingestion successful!"));
                    } catch (Exception e) {
                        e.printStackTrace();
                        return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data ingestion!"));
                    }
                }
                //GET application data
                else if (myApp != null && matches(buf, req.path, URI_APP_GET)) {
                    HashMap<String, String> result = new HashMap<>();
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            ArrayList<String> ids = new ArrayList<>();
                            if (obj.has("key")) {
                                JSONArray tmpKeys = obj.getJSONArray("key");
                                ids.addAll(tmpKeys.toList().stream().map(Object::toString).collect(Collectors.toList()));
                            }
                            result = extractAppData(ids);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    //Join meta with values
                    List<String> metaValues = new ArrayList<>();
                    for (String metric : result.keySet()) {
                        metaValues.add(" { \"key\": \"" + metric + "\" , \"value\": \"" + result.get(metric) + "\" } ");
                    }
                    String finalRes = "{\"application\": [ " + String.join(", ", metaValues) + " ]} ";
                    return json(ctx, req.isKeepAlive.value, finalRes.getBytes());
                }
            }
            return HttpStatus.NOT_FOUND;
        }
    }

    //-----------PUBLIC FUNCTIONS--------------
    @Override
    //Extract latest
    public ArrayList<String> extractMonitoring(HashMap<String, HashSet<String>> ids) {
        ArrayList<String> result = new ArrayList<>();
        HashMap<MetricKey, String> metaValues = extractMeta(ids);
        for (MetricKey myKey: metaValues.keySet()){
            String data = extractLatestData(myKey);
            result.add(" { \"metricID\": \"" + myKey.metricID + "\" , \"entityID\": \"" + myKey.entityID + "\", " + data + " , " + metaValues.getOrDefault(myKey, "") + " } ");
        }
        return result;
    }

    @Override
    //Extract historical
    public ArrayList<String> extractMonitoring(HashMap<String, HashSet<String>> ids, Long from, Long to) {
        ArrayList<String> result = new ArrayList<>();
        HashMap<MetricKey, String> metaValues = extractMeta(ids);
        for (MetricKey myKey: metaValues.keySet()){
            String data = extractHistoricalData(myKey, from, to);
            result.add(" { \"metricID\": \"" + myKey.metricID + "\" , \"entityID\": \"" + myKey.entityID + "\", " + data + " , " + metaValues.getOrDefault(myKey, "") + " } ");
        }
        return result;
    }

    @Override
    //Extract analytics
    public HashMap<String, String> extractAnalytics(Set<String> ids) {
        HashMap<String, String> result = extractAnalyticsData(ids);
        result.replaceAll((k, v) -> "{ \"key\": \"" + k + "\", " + result.get(k) + " } ");
        return result;
    }
}
