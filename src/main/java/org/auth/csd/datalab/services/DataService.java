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
import org.auth.csd.datalab.common.models.keys.AnalyticKey;
import org.auth.csd.datalab.common.models.keys.MetricKey;
import org.auth.csd.datalab.common.models.keys.MetricTimeKey;
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
import java.nio.charset.StandardCharsets;
import java.sql.Time;
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
    private IgniteCache<AnalyticKey, Metric> myAnalytics;
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

    //------------Common----------------
    private FieldsQueryCursor<List<?>> getQueryValues(IgniteCache cache, String sql){
        SqlFieldsQuery query = new SqlFieldsQuery(sql);
        return cache.query(query);
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
    private void ingestAnalytics(HashMap<AnalyticKey, Metric> data) {
        for (Map.Entry<AnalyticKey, Metric> entry : data.entrySet()) {
            myAnalytics.put(entry.getKey(), entry.getValue());
        }
    }

    private String beautifyAnalytics(String key, List<TimedMetric> values){
        StringBuilder result = new StringBuilder("{\"key\": \"" + key + "\",");
        result.append("\"values\": [");
        if(!values.isEmpty()) {
            values.forEach(v -> result.append("{").append(v).append("},"));
            result.deleteCharAt(result.length()-1);
        }
        result.append("]}");
        return result.toString();
    }

    private List<TimedMetric> extractAnalyticsData(String key) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT a.* " +
                "FROM METRIC AS a " +
                "INNER JOIN (SELECT key, MAX(timestamp) as timestamp FROM METRIC GROUP BY key) AS b ON a.key = b.key AND a.timestamp = b.timestamp " +
                "WHERE a.key = '" + key + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myAnalytics, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double)row.get(2), (long)row.get(1)));
        }
        return result;
    }

    private List<TimedMetric> extractAnalyticsData(String key, long from, long to) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT key, timestamp, val FROM METRIC WHERE timestamp BETWEEN " + from + " AND " + to + " AND key = '" + key + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myAnalytics, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double)row.get(2), (long)row.get(1)));
        }
        return result;
    }

    //------------MONITORING----------------
    private void ingestMetric(HashMap<MetricKey, InputJson> data) {
        for (Map.Entry<MetricKey, InputJson> entry : data.entrySet()) {
            myMeta.put(entry.getKey(), new MetaMetric(entry.getValue()));
            myLatest.put(entry.getKey(), new TimedMetric(entry.getValue().val, entry.getValue().timestamp));
            myHistorical.put(new MetricTimeKey(entry.getValue()), new Metric(entry.getValue().val));
        }
    }

    private String beautifyMonitoring(MetricKey key, MetaMetric metadata, List<TimedMetric> values){
        StringBuilder result = new StringBuilder("{" + key + ",");
        result.append("\"values\": [");
        if(!values.isEmpty()) {
            values.forEach(v -> result.append("{").append(v).append("},"));
            result.deleteCharAt(result.length()-1);
        }
        result.append("],").append(metadata).append("}");
        return result.toString();
    }

    private List<TimedMetric> extractMonitoringData(MetricKey key){
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT timestamp, val FROM TIMEDMETRIC WHERE metricID = '" + key.metricID + "' AND entityID = '" + key.entityID + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myLatest, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double)row.get(1), (long)row.get(0)));
        }
        //If main memory cache is empty (due to restart)
        if(result.isEmpty()){
            sql = "SELECT a.timestamp, a.val " +
                    "FROM METRIC AS a " +
                    "INNER JOIN (SELECT metricID, entityID, MAX(timestamp) as timestamp FROM METRIC GROUP BY (metricID, entityID)) AS b ON a.metricID = b.metricID AND a.timestamp = b.timestamp AND a.entityID = b.entityID " +
                    "WHERE a.metricID = '" + key.metricID + "' AND a.entityID = '" + key.entityID + "'";
            // Iterate over the result set.
            try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, sql)) {
                for (List<?> row : cursor)
                    result.add(new TimedMetric((Double)row.get(1), (long)row.get(0)));
            }
        }
        return result;
    }

    private List<TimedMetric> extractMonitoringData(MetricKey key, long from, long to){
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT timestamp, val FROM METRIC WHERE metricID = '" + key.metricID + "' AND entityID = '" + key.entityID + "' AND timestamp BETWEEN " + from + " AND " + to ;
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double)row.get(1), (long)row.get(0)));
        }
        return result;
    }

    private HashMap<MetricKey, MetaMetric> extractMetaData(HashMap<String, HashSet<String>> ids) {
        String select = "SELECT metricID, entityID, entityType, name, units, desc, groupName, minVal, maxVal, higherIsBetter, podUUID, podName, podNamespace, containerID, containerName ";
        String from = "FROM METAMETRIC ";
        List<String> where = new ArrayList<>();
        HashMap<MetricKey, MetaMetric> result = new HashMap<>();
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
        try (QueryCursor<List<?>> cursor = getQueryValues(myMeta, select + from + finalWhere)) {
            for (List<?> row : cursor){
                MetricKey key = new MetricKey(row.get(0).toString(), row.get(1).toString());
                MetaMetric value = new MetaMetric(row.get(2).toString(),row.get(3).toString(),row.get(4).toString(),row.get(5).toString(),
                        row.get(6).toString(),(double)row.get(7),(double)row.get(8),(boolean)row.get(9),
                        row.get(10).toString(),row.get(11).toString(),row.get(12).toString(),row.get(13).toString(),
                        row.get(14).toString());
                result.put(key, value);
            }
        }
        return result;
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
                    StringBuilder result = new StringBuilder("{\"monitoring\": [");
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
                                    String nodeData = (from > -1) ? extractionInterface.extractMonitoringJson(filters, from, to) : extractionInterface.extractMonitoringJson(filters);
                                    if (nodeData != null)
                                        result.append("{\"node\": \"").append(server.hostNames()).append("\", \"data\": [").append(nodeData).append("]}");
                                }
                            } else { //Get local data
                                if(from > -1) result.append(extractMonitoringJson(filters, from, to));
                                else result.append(extractMonitoringJson(filters));
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
                    HashMap<AnalyticKey, Metric> data = new HashMap<>();
                    //Read and parse json
                    try {
                        String body = buf.get(req.body);
                        JSONObject obj = new JSONObject(body);
                        //Get the monitoring keyword and parse the data
                        JSONArray analytics = obj.getJSONArray("analytics");
                        for (int i = 0; i < analytics.length(); i++) {
                            JSONObject o = analytics.getJSONObject(i);
                            if (o.has("key") && o.has("val") && o.has("timestamp")) {
                                data.put(new AnalyticKey(o.getString("key"), o.getLong("timestamp")), new Metric(o.getDouble("val")));
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
                    StringBuilder result = new StringBuilder("{\"analytics\": [");
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
                            long from = -1, to = -1;
                            if (obj.has("from") && obj.has("to") && obj.getLong("from") >= 0 && obj.getLong("to") >= obj.getLong("from")) {
                                from = obj.getLong("from");
                                to = obj.getLong("to");
                            }
                            if(from > -1) result.append(extractAnalyticsJson(ids, from, to));
                            else result.append(extractAnalyticsJson(ids));
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    result.append("]}");
                    return json(ctx, req.isKeepAlive.value, result.toString().getBytes());
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
    //Extract latest monitoring
    public HashMap<MetricKey, Monitoring> extractMonitoring(HashMap<String, HashSet<String>> filter) {
        HashMap<MetricKey, Monitoring> result = new HashMap<>();
        HashMap<MetricKey, MetaMetric> metaValues = extractMetaData(filter);
        for (MetricKey myKey: metaValues.keySet()){
            List<TimedMetric> data = extractMonitoringData(myKey);
            result.put(myKey, new Monitoring(metaValues.get(myKey), data));
        }
        return result;
    }

    @Override
    //Extract historical monitoring
    public HashMap<MetricKey, Monitoring> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to) {
        HashMap<MetricKey, Monitoring> result = new HashMap<>();
        HashMap<MetricKey, MetaMetric> metaValues = extractMetaData(filter);
        for (MetricKey myKey: metaValues.keySet()){
            List<TimedMetric> data = extractMonitoringData(myKey, from, to);
            result.put(myKey, new Monitoring(metaValues.get(myKey), data));
        }
        return result;
    }

    @Override
    public String extractMonitoringJson(HashMap<String, HashSet<String>> filter) {
        StringBuilder result = new StringBuilder();
        HashMap<MetricKey, Monitoring> data = extractMonitoring(filter);
        if(!data.isEmpty()) {
            data.forEach((k, v) -> result.append(beautifyMonitoring(k, v.metadata, v.values)).append(","));
            result.deleteCharAt(result.length() - 1);
        }
        return result.toString();
    }

    @Override
    public String extractMonitoringJson(HashMap<String, HashSet<String>> filter, Long from, Long to) {
        StringBuilder result = new StringBuilder();
        HashMap<MetricKey, Monitoring> data = extractMonitoring(filter, from, to);
        if(!data.isEmpty()) {
            data.forEach((k, v) -> result.append(beautifyMonitoring(k, v.metadata, v.values)).append(","));
            result.deleteCharAt(result.length() - 1);
        }
        return result.toString();
    }

    @Override
    public HashMap<String, List<TimedMetric>> extractAnalytics(Set<String> filter) {
        HashMap<String, List<TimedMetric>> result = new HashMap<>();
        for (String key: filter) result.put(key, extractAnalyticsData(key));
        return result;
    }

    @Override
    public HashMap<String, List<TimedMetric>> extractAnalytics(Set<String> filter, long from, long to) {
        HashMap<String, List<TimedMetric>> result = new HashMap<>();
        for (String key: filter) result.put(key, extractAnalyticsData(key, from, to));
        return result;
    }

    @Override
    public String extractAnalyticsJson(Set<String> keys){
        StringBuilder result = new StringBuilder();
        HashMap<String, List<TimedMetric>> data = extractAnalytics(keys);
        if(!data.isEmpty()) {
            data.forEach((k, v) -> result.append(beautifyAnalytics(k, v)).append(","));
            result.deleteCharAt(result.length() - 1);
        }
        return result.toString();
    }

    @Override
    public String extractAnalyticsJson(Set<String> keys, long from, long to){
        StringBuilder result = new StringBuilder();
        HashMap<String, List<TimedMetric>> data = extractAnalytics(keys, from, to);
        if(!data.isEmpty()) {
            data.forEach((k, v) -> result.append(beautifyAnalytics(k, v)).append(","));
            result.deleteCharAt(result.length() - 1);
        }
        return result.toString();
    }

}
