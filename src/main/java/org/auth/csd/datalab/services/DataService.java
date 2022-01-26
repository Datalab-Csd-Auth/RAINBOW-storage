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
    private ArrayList<String> aggregations = new ArrayList<String>() {
        {
            add("max");
            add("min");
            add("sum");
            add("avg");
        }
    };

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
    private FieldsQueryCursor<List<?>> getQueryValues(IgniteCache cache, String sql) {
        SqlFieldsQuery query = new SqlFieldsQuery(sql);
        return cache.query(query);
    }

    private HashMap<String, HashSet<String>> getFilters(JSONObject obj){
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

    private List<TimedMetric> extractAnalyticsData(String key) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT a.* " +
                "FROM METRIC AS a " +
                "INNER JOIN (SELECT key, MAX(timestamp) as timestamp FROM METRIC GROUP BY key) AS b ON a.key = b.key AND a.timestamp = b.timestamp " +
                "WHERE a.key = '" + key + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myAnalytics, sql)) {
            for (List<?> row : cursor) {
                result.add(new TimedMetric((Double) row.get(2), (long) row.get(1)));
            }
        }
        return result;
    }

    private List<TimedMetric> extractAnalyticsData(String key, long from, long to) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT key, timestamp, val FROM METRIC WHERE timestamp BETWEEN " + from + " AND " + to + " AND key = '" + key + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myAnalytics, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double) row.get(2), (long) row.get(1)));
        }
        return result;
    }

    private boolean deleteAnalytics(HashSet<String> ids){
        String sql = "DELETE " +
                "FROM METRIC " +
                "WHERE key IN  ('" + String.join("','", ids) + "')";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myAnalytics, sql)) {
            for (List<?> row : cursor) {
                return !((Long)row.get(0) == 0);
            }
        }
        return false;
    }

    //------------MONITORING----------------
    private void ingestMetric(HashMap<MetricKey, InputJson> data) {
        for (Map.Entry<MetricKey, InputJson> entry : data.entrySet()) {
            myMeta.put(entry.getKey(), new MetaMetric(entry.getValue()));
            myLatest.put(entry.getKey(), new TimedMetric(entry.getValue().val, entry.getValue().timestamp));
            myHistorical.put(new MetricTimeKey(entry.getValue()), new Metric(entry.getValue().val));
        }
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

    private List<TimedMetric> extractMonitoringData(MetricKey key) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT timestamp, val FROM TIMEDMETRIC WHERE metricID = '" + key.metricID + "' AND entityID = '" + key.entityID + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myLatest, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double) row.get(1), (long) row.get(0)));
        }
        //If main memory cache is empty (due to restart)
        if (result.isEmpty()) {
            sql = "SELECT a.timestamp, a.val " +
                    "FROM METRIC AS a " +
                    "INNER JOIN (SELECT metricID, entityID, MAX(timestamp) as timestamp FROM METRIC GROUP BY (metricID, entityID)) AS b ON a.metricID = b.metricID AND a.timestamp = b.timestamp AND a.entityID = b.entityID " +
                    "WHERE a.metricID = '" + key.metricID + "' AND a.entityID = '" + key.entityID + "'";
            // Iterate over the result set.
            try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, sql)) {
                for (List<?> row : cursor)
                    result.add(new TimedMetric((Double) row.get(1), (long) row.get(0)));
            }
        }
        return result;
    }

    private List<TimedMetric> extractMonitoringData(MetricKey key, long from, long to) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT timestamp, val FROM METRIC WHERE metricID = '" + key.metricID + "' AND entityID = '" + key.entityID + "' AND timestamp BETWEEN " + from + " AND " + to;
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double) row.get(1), (long) row.get(0)));
        }
        return result;
    }

    private List<TimedMetric> extractMonitoringData(MetricKey key, long from, long to, String agg) {
        List<TimedMetric> result = new ArrayList<>();
        String select = "SELECT ";
        switch (agg) {
            case "min":
                select += "MIN(val) ";
                break;
            case "max":
                select += "MAX(val) ";
                break;
            case "sum":
                select += "SUM(val) ";
                break;
            case "avg":
                select += "SUM(val), COUNT(val) ";
                break;
        }
        String rest = "FROM METRIC WHERE metricID = '" + key.metricID + "' AND entityID = '" + key.entityID + "' AND timestamp BETWEEN " + from + " AND " + to;
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, select + rest)) {
            for (List<?> row : cursor)
                if (agg.equals("avg"))
                    result.add(new TimedMetric((Double) row.get(0), (long) row.get(1)));
                else
                    result.add(new TimedMetric((Double) row.get(0), null));
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
                if (!exact.isEmpty())
                    tmpWhere.add(entry.getKey() + " IN ('" + String.join("','", entry.getValue()) + "') ");
                for (String key : wildcard) {
                    tmpWhere.add(entry.getKey() + " LIKE '" + key + "' ");
                }
                where.add("(" + String.join(") OR (", tmpWhere) + ")");
            }
        }
        String finalWhere = (!where.isEmpty()) ? " WHERE (" + String.join(") AND (", where) + ") " : "";
        try (QueryCursor<List<?>> cursor = getQueryValues(myMeta, select + from + finalWhere)) {
            for (List<?> row : cursor) {
                MetricKey key = new MetricKey(row.get(0).toString(), row.get(1).toString());
                MetaMetric value = new MetaMetric(row.get(2).toString(), row.get(3).toString(), row.get(4).toString(), row.get(5).toString(),
                        row.get(6).toString(), (double) row.get(7), (double) row.get(8), (boolean) row.get(9),
                        row.get(10).toString(), row.get(11).toString(), row.get(12).toString(), row.get(13).toString(),
                        row.get(14).toString());
                result.put(key, value);
            }
        }
        return result;
    }

    //------------API----------------
    private class CustomHttpServer extends AbstractHttpServer {

        //Requests
        private final byte[] REQ_POST = "POST".getBytes();
        private final byte[] REQ_GET = "GET".getBytes();
        private final byte[] REQ_DEL = "DELETE".getBytes();
        //Helpers
        private final byte[] URI_NODES = "/nodes".getBytes();
        //Monitoring
        private final byte[] URI_PUT = "/put".getBytes();
        private final byte[] URI_GET = "/get".getBytes();
        private final byte[] URI_LIST = "/list".getBytes();
        //Analytics cache
        private final byte[] URI_ANALYTICS = "/analytics".getBytes();
        private final byte[] URI_ANALYTICS_PUT = "/analytics/put".getBytes();
        private final byte[] URI_ANALYTICS_GET = "/analytics/get".getBytes();
        //App cache
        private final byte[] URI_APP_PUT = "/app/put".getBytes();
        private final byte[] URI_APP_GET = "/app/get".getBytes();

        @Override
        protected HttpStatus handle(Channel ctx, Buf buf, RapidoidHelper req) {
            if (matches(buf, req.verb, REQ_POST)) {
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
                            HashMap<String, HashSet<String>> filters = getFilters(obj);
                            //Get nodes
                            ClusterGroup servers = null;
                            if (obj.has("nodes")) { //Get data from the cluster
                                JSONArray nodes = obj.getJSONArray("nodes");
                                HashSet<String> nodesList = nodes.toList().stream().map(Object::toString).collect(Collectors.toCollection(HashSet::new));
                                servers = (nodesList.isEmpty()) ? ignite.cluster().forServers() : ignite.cluster().forServers().forPredicate(getNodesByHostnames(nodesList));
                            }
                            //Get aggregations
                            if (obj.has("agg") && aggregations.contains(obj.getString("agg"))) {
                                String aggreg = obj.getString("agg");
                                //Get values and beautify
                                if (servers == null) {
                                    HashMap<MetricKey, Monitoring> data = extractMonitoring(filters, from, to, aggreg);
                                    if (!data.isEmpty()) {
                                        for (MetricKey key : data.keySet()) {
                                            if (aggreg.equals("avg")) {
                                                Monitoring oldMon = data.get(key);
                                                double finalValue = oldMon.values.get(0).val / oldMon.values.get(0).timestamp;
                                                List<TimedMetric> tmp = new ArrayList<>();
                                                tmp.add(new TimedMetric(finalValue, null));
                                                Monitoring newMon = new Monitoring(oldMon.metadata, tmp);
                                                data.replace(key, newMon);
                                            }
                                            result.append(beautifyMonitoring(key, data.get(key).metadata, data.get(key).values)).append(",");
                                        }
                                        result.deleteCharAt(result.length() - 1);
                                    }
                                } else {
                                    HashMap<MetricKey, Monitoring> res = null;
                                    for (ClusterNode server : servers.nodes()) {
                                        DataInterface extractionInterface = ignite.services(ignite.cluster().forNodeId(server.id())).serviceProxy(DataInterface.SERVICE_NAME,
                                                DataInterface.class, false);
                                        if (res == null) //If this is the first node checked
                                            res = extractionInterface.extractMonitoring(filters, from, to, aggreg);
                                        else { //If this is a node after the first one
                                            HashMap<MetricKey, Monitoring> data = extractionInterface.extractMonitoring(filters, from, to, aggreg);
                                            if (!data.isEmpty()) {
                                                for (MetricKey key : data.keySet()) {
                                                    if (res.containsKey(key)) {
                                                        Double oldVal = res.get(key).values.get(0).val;
                                                        Double newVal = data.get(key).values.get(0).val;
                                                        switch (aggreg) {
                                                            case "sum": {
                                                                res.get(key).values.get(0).val += data.get(key).values.get(0).val;
                                                                break;
                                                            }
                                                            case "max": {
                                                                if (newVal > oldVal)
                                                                    res.get(key).values.get(0).val = newVal;
                                                                break;
                                                            }
                                                            case "min": {
                                                                if (newVal < oldVal)
                                                                    res.get(key).values.get(0).val = newVal;
                                                                break;
                                                            }
                                                            case "avg": {
                                                                res.get(key).values.get(0).val += newVal;
                                                                res.get(key).values.get(0).timestamp += data.get(key).values.get(0).timestamp;
                                                                break;
                                                            }
                                                        }
                                                    } else res.put(key, data.get(key));
                                                }
                                            }
                                        }
                                    }
                                    if (res != null) {
                                        for (MetricKey key : res.keySet()) {
                                            if (aggreg.equals("avg")) {
                                                Monitoring oldMon = res.get(key);
                                                double finalValue = oldMon.values.get(0).val / oldMon.values.get(0).timestamp;
                                                List<TimedMetric> tmp = new ArrayList<>();
                                                tmp.add(new TimedMetric(finalValue, null));
                                                Monitoring newMon = new Monitoring(oldMon.metadata, tmp);
                                                res.replace(key, newMon);
                                            }
                                            result.append(beautifyMonitoring(key, res.get(key).metadata, res.get(key).values)).append(",");
                                            result.deleteCharAt(result.length() - 1);
                                        }
                                    }
                                }
                            }
                            //Get values
                            else {
                                if (servers != null) { //Get data from the cluster
                                    for (ClusterNode server : servers.nodes()) {
                                        DataInterface extractionInterface = ignite.services(ignite.cluster().forNodeId(server.id())).serviceProxy(DataInterface.SERVICE_NAME,
                                                DataInterface.class, false);
                                        String nodeData = (from > -1) ? extractionInterface.extractMonitoringJson(filters, from, to) : extractionInterface.extractMonitoringJson(filters);
                                        if (nodeData != null)
                                            result.append("{\"node\": \"").append(server.hostNames()).append("\", \"data\": [").append(nodeData).append("]}");
                                    }
                                } else { //Get local data
                                    if (from > -1) result.append(extractMonitoringJson(filters, from, to));
                                    else result.append(extractMonitoringJson(filters));
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    result.append("]}");
                    return json(ctx, req.isKeepAlive.value, result.toString().getBytes());
                }
                //get LIST of monitoring metrics
                else if (matches(buf, req.path, URI_LIST)) {
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
                            //Get server list
                            ClusterGroup servers = ignite.cluster().forServers();
                            //Get data from the cluster
                            HashMap<MetricKey, MetaMetric> metricData = new HashMap<>();
                            for (ClusterNode server : servers.nodes()) {
                                DataInterface extractionInterface = ignite.services(ignite.cluster().forNodeId(server.id())).serviceProxy(DataInterface.SERVICE_NAME,
                                        DataInterface.class, false);
                                metricData.putAll(extractionInterface.extractMeta(filters));
                            }
                            if(!metricData.isEmpty()) {
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
                            if (from > -1) result.append(extractAnalyticsJson(ids, from, to));
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
                //GET list of nodes
                else if(matches(buf, req.path, URI_NODES)){
                    StringBuilder result = new StringBuilder("{\"nodes\": [");
                    ClusterGroup servers = ignite.cluster().forServers();
                    servers.nodes().forEach(s -> result
                            .append("{\"id\": \"")
                            .append(s.consistentId())
                            .append("\",\"hostname\": \"")
                            .append(s.hostNames())
                            .append("\",")
                            .append("\"cluster_head\": ")
                            .append((Boolean) s.attribute("data.head"))
                            .append("},"));
                    if(servers.nodes().size() > 0) result.deleteCharAt(result.length() - 1);
                    result.append("]}");
                    return json(ctx, req.isKeepAlive.value, result.toString().getBytes());
                }
            }else if (matches(buf, req.verb, REQ_DEL)){
                //Delete analytics
                if (matches(buf, req.path, URI_ANALYTICS)) {
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
                            //Delete stuff
                            if(!ids.isEmpty() && deleteAnalytics(ids)) return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("OK", ""));
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", ""));
                        }
                    }
                    return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", ""));
                }
            }
            return HttpStatus.NOT_FOUND;
        }
    }

    //-----------PUBLIC FUNCTIONS--------------

    @Override
    //Extract only meta data
    public HashMap<MetricKey, MetaMetric> extractMeta(HashMap<String, HashSet<String>> filter) {
        return extractMetaData(filter);
    }

    @Override
    //Extract latest monitoring data
    public HashMap<MetricKey, Monitoring> extractMonitoring(HashMap<String, HashSet<String>> filter) {
        HashMap<MetricKey, Monitoring> result = new HashMap<>();
        HashMap<MetricKey, MetaMetric> metaValues = extractMetaData(filter);
        for (MetricKey myKey : metaValues.keySet()) {
            List<TimedMetric> data = extractMonitoringData(myKey);
            result.put(myKey, new Monitoring(metaValues.get(myKey), data));
        }
        return result;
    }

    @Override
    //Extract historical monitoring data
    public HashMap<MetricKey, Monitoring> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to) {
        HashMap<MetricKey, Monitoring> result = new HashMap<>();
        HashMap<MetricKey, MetaMetric> metaValues = extractMetaData(filter);
        for (MetricKey myKey : metaValues.keySet()) {
            List<TimedMetric> data = extractMonitoringData(myKey, from, to);
            result.put(myKey, new Monitoring(metaValues.get(myKey), data));
        }
        return result;
    }

    @Override
    //Extract historical monitoring data with aggregation
    public HashMap<MetricKey, Monitoring> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to, String agg) {
        HashMap<MetricKey, Monitoring> result = new HashMap<>();
        HashMap<MetricKey, MetaMetric> metaValues = extractMetaData(filter);
        for (MetricKey myKey : metaValues.keySet()) {
            List<TimedMetric> data = extractMonitoringData(myKey, from, to, agg);
            result.put(myKey, new Monitoring(metaValues.get(myKey), data));
        }
        return result;
    }

    @Override
    //Extract latest monitoring as json string
    public String extractMonitoringJson(HashMap<String, HashSet<String>> filter) {
        StringBuilder result = new StringBuilder();
        HashMap<MetricKey, Monitoring> data = extractMonitoring(filter);
        if (!data.isEmpty()) {
            data.forEach((k, v) -> result.append(beautifyMonitoring(k, v.metadata, v.values)).append(","));
            result.deleteCharAt(result.length() - 1);
        }
        return result.toString();
    }

    @Override
    //Extract historical monitoring as json string
    public String extractMonitoringJson(HashMap<String, HashSet<String>> filter, Long from, Long to) {
        StringBuilder result = new StringBuilder();
        HashMap<MetricKey, Monitoring> data = extractMonitoring(filter, from, to);
        if (!data.isEmpty()) {
            data.forEach((k, v) -> result.append(beautifyMonitoring(k, v.metadata, v.values)).append(","));
            result.deleteCharAt(result.length() - 1);
        }
        return result.toString();
    }

    @Override
    //Extract latest analytics data
    public HashMap<String, List<TimedMetric>> extractAnalytics(Set<String> filter) {
        HashMap<String, List<TimedMetric>> result = new HashMap<>();
        for (String key : filter) {
            List<TimedMetric> tmpRes = extractAnalyticsData(key);
            if(!tmpRes.isEmpty()) result.put(key, tmpRes);
        }
        return result;
    }

    @Override
    //Extract historical analytics data
    public HashMap<String, List<TimedMetric>> extractAnalytics(Set<String> filter, long from, long to) {
        HashMap<String, List<TimedMetric>> result = new HashMap<>();
        for (String key : filter) {
            List<TimedMetric> tmpRes = extractAnalyticsData(key, from, to);
            if(!tmpRes.isEmpty()) result.put(key, tmpRes);
        }
        return result;
    }

    @Override
    //Extract latest analytics as json string
    public String extractAnalyticsJson(Set<String> keys) {
        StringBuilder result = new StringBuilder();
        HashMap<String, List<TimedMetric>> data = extractAnalytics(keys);
        if (!data.isEmpty()) {
            data.forEach((k, v) -> result.append(beautifyAnalytics(k, v)).append(","));
            result.deleteCharAt(result.length() - 1);
        }
        return result.toString();
    }

    @Override
    //Extract historical analytics as json string
    public String extractAnalyticsJson(Set<String> keys, long from, long to) {
        StringBuilder result = new StringBuilder();
        HashMap<String, List<TimedMetric>> data = extractAnalytics(keys, from, to);
        if (!data.isEmpty()) {
            data.forEach((k, v) -> result.append(beautifyAnalytics(k, v)).append(","));
            result.deleteCharAt(result.length() - 1);
        }
        return result.toString();
    }

}
