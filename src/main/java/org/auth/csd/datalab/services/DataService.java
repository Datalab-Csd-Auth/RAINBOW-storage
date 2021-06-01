package org.auth.csd.datalab.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
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

public class DataService implements DataInterface {

    @IgniteInstanceResource
    private Ignite ignite;
    /**
     * Reference to the cache.
     */
    private IgniteCache<String, TimedMetric> myLatest;
    private IgniteCache<MetricKey, Metric> myHistorical;
    private IgniteCache<MetaMetricKey, MetaMetric> myMeta;
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
        if(app_cache) myApp = ignite.cache(appCacheName);
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
        myApp.query(new ScanQuery<>(filter)).forEach(entry -> {
            data.put(entry.getKey(),entry.getValue());
        });
        return data;
    }

    //------------ANALYTICS----------------
    private void ingestAnalytics(HashMap<String, TimedMetric> data) {
        for (Map.Entry<String, TimedMetric> entry : data.entrySet()) {
            myAnalytics.put(entry.getKey(), entry.getValue());
        }
    }

    private HashMap<String, String> extractAnalyticsData(List<String> search) {
        HashMap<String, String> data = new HashMap<>();
        IgniteBiPredicate<String, TimedMetric> filter;
        if (!search.isEmpty()) filter = (key, val) -> search.contains(key);
        else filter = null;
        myAnalytics.query(new ScanQuery<>(filter)).forEach(entry -> {
            data.put(entry.getKey(),"\"val\": " + entry.getValue().val + " , \"timestamp\": " + entry.getValue().timestamp);
        });
        return data;
    }

    //------------MONITORING----------------
    private void ingestMetric(HashMap<String, InputJson> data) {
        for (Map.Entry<String, InputJson> entry : data.entrySet()) {
            myLatest.put(entry.getKey(), new TimedMetric(entry.getValue().val, entry.getValue().timestamp));
            MetricKey metricTmp = new MetricKey(entry.getValue());
            myHistorical.put(metricTmp, new Metric(entry.getValue().val));
            myMeta.put(new MetaMetricKey(entry.getValue()), new MetaMetric(entry.getValue()));
        }
    }

    private HashMap<String, String> extractLatestData(HashSet<String> search) {
        HashMap<String, String> data = new HashMap<>();
        IgniteBiPredicate<String, TimedMetric> filter;
        if (!search.isEmpty()) filter = (key, val) -> search.contains(key);
        else filter = null;
        myLatest.query(new ScanQuery<>(filter)).forEach(entry -> {
            data.put(entry.getKey(), "\"val\": " + entry.getValue().val + " , \"timestamp\": " + entry.getValue().timestamp);
        });
        return data;
    }

    private String extractHistoricalData(String metric, Long min, Long max) {
        List<String> res = new ArrayList<>();
        SqlFieldsQuery sql = new SqlFieldsQuery("select metricID, timestamp, val from METRIC WHERE metricID = '" + metric + "' AND timestamp >= " + min + " AND timestamp <= " + max);
        try (QueryCursor<List<?>> cursor = myHistorical.query(sql)) {
            for (List<?> row : cursor) {
                Long time = (Long) row.get(1);
                Double value = (Double) row.get(2);
                res.add(" { \"timestamp\": " + time + " , \"val\": " + value + " } ");
            }
        }
        String result = "\"values\": [ " + String.join(", ", res) + " ] ";
        return result;
    }

    private HashMap<String, String> extractMeta(HashSet<String> search) {
        HashMap<String, String> meta = new HashMap<>();
        for (String metric : search) {
            SqlFieldsQuery sql = new SqlFieldsQuery("select entityID, entityType, name, units, desc, groupName, minVal, maxVal, higherIsBetter from METAMETRIC WHERE metricID = '" + metric + "'");
            try (QueryCursor<List<?>> cursor = myMeta.query(sql)) {
                for (List<?> row : cursor) {
                    String res = " \"entityID\": \"" + row.get(0) + "\"" +
                            ", \"entityType\": \"" + row.get(1) + "\"" +
                            ", \"name\": \"" + row.get(2) + "\"" +
                            ", \"units\": \"" + row.get(3) + "\"" +
                            ", \"desc\": \"" + row.get(4) + "\"" +
                            ", \"group\": \"" + row.get(5) + "\"" +
                            ", \"minVal\": " + row.get(6) +
                            ", \"maxVal\": " + row.get(7) +
                            ", \"higherIsBetter\": " + row.get(8) + " ";
                    meta.put(metric, res);
                }
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
                    HashMap<String, InputJson> metrics = new HashMap<>();
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
                                metrics.put(myMetric.metricID, myMetric);
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
                    HashMap<String, String> result = new HashMap<>();
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
                                    result = extractMonitoring(ids, entityFlag);
                                } else if (obj.has("from") && obj.has("to")) {
                                    long from = obj.getLong("from");
                                    long to = obj.getLong("to");
                                    result = extractMonitoring(ids, entityFlag, from, to);
                                }
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    String finalRes = "{\"monitoring\": [ " + String.join(", ", result.values()) + " ]} ";
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
                            if(o.has("key") && o.has("val") && o.has("timestamp")){
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
                            ArrayList<String> ids = new ArrayList<>();
                            if (obj.has("key")) {
                                JSONArray tmpKeys = obj.getJSONArray("key");
                                ids.addAll(tmpKeys.toList().stream().map(Object::toString).collect(Collectors.toList()));
                            }
                            result = extractAnalyticsData(ids);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    //Join meta with values
                    List<String> metaValues = new ArrayList<>();
                    for (String metric : result.keySet()) {
                        metaValues.add(" { \"key\": \"" + metric + "\" , " + result.get(metric) + " } ");
                    }
                    String finalRes = "{\"analytics\": [ " + String.join(", ", metaValues) + " ]} ";
                    return json(ctx, req.isKeepAlive.value, finalRes.getBytes());
                }
                //PUT application data
                else if (myApp != null && matches(buf, req.path, URI_APP_PUT)){
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
                            if(o.has("key") && o.has("value")){
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
    public HashMap<String, String> extractMonitoring(Set<String> ids, boolean entity) {
        HashMap<String,String> metaValues = new HashMap<>();
        HashSet<String> metrics = new HashSet<>();
        if(ids.isEmpty()) metrics.addAll(getMetricID());
        else if(!entity) metrics.addAll(ids);
        else metrics.addAll(getMetricID(new ArrayList<>(ids)));
        if (!metrics.isEmpty()) {
            HashMap<String, String> values = extractLatestData(metrics);
            HashMap<String, String> meta = extractMeta(metrics);
            for (String metric : values.keySet()) {
                metaValues.put(metric, " { \"metricID\": \"" + metric + "\" , " + values.get(metric) + " , " + meta.getOrDefault(metric, "") + " } ");
            }
        }
        return metaValues;
    }

    @Override
    //Extract historical
    public HashMap<String, String> extractMonitoring(Set<String> ids, boolean entity, Long from, Long to) {
        HashMap<String,String> metaValues = new HashMap<>();
        HashSet<String> metrics = new HashSet<>();
        if(ids.isEmpty()) metrics.addAll(getMetricID());
        else if(!entity) metrics.addAll(ids);
        else metrics.addAll(getMetricID(new ArrayList<>(ids)));
        if (!metrics.isEmpty()) {
            HashMap<String, String> values = new HashMap<>();
            for (String id : metrics){
                values.put(id,extractHistoricalData(id, from, to));
            }
            HashMap<String, String> meta = extractMeta(metrics);

            for (String metric : values.keySet()) {
                metaValues.put(metric, " { \"metricID\": \"" + metric + "\" , " + values.get(metric) + " , " + meta.getOrDefault(metric, "") + " } ");
            }
        }
        return metaValues;
    }
}
