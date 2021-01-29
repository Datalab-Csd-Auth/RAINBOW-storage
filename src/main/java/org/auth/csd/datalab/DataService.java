package org.auth.csd.datalab;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.helpers.*;
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

public class DataService implements DataInterface {

    //TODO ENV variables
    private String latestCacheName = "LatestMonitoring";
    private String historicalCacheName = "HistoricalMonitoring";
    private String metaCacheName = "MetaMonitoring";
    @IgniteInstanceResource
    private Ignite ignite;
    /**
     * Reference to the cache.
     */
    private IgniteCache<String, TimedMetric> myLatest;
    private IgniteCache<MetricKey, Metric> myHistorical;
    private IgniteCache<MetaMetricKey, MetaMetric> myMeta;
    UUID localNode = null;
    private String delimiter = ".";
    private Server server;

    /**
     * {@inheritDoc}
     */
    public void init(ServiceContext ctx) {
        System.out.println("Initializing Data Service on node:" + ignite.cluster().localNode());
        //TODO set persistence
        //Get the cache that is designed in the config for the latest data
        myLatest = ignite.cache(latestCacheName);
        //Create a cache for the meta data
        CacheConfiguration<MetaMetricKey, MetaMetric> metaCfg = new CacheConfiguration<>(metaCacheName);
        metaCfg.setIndexedTypes(MetaMetricKey.class, MetaMetric.class);
        metaCfg.setCacheMode(CacheMode.LOCAL);
        //metaCfg.setDataRegionName("Persistent_Region");
        myMeta = ignite.getOrCreateCache(metaCfg);
//        Create a cache for the historical data
        CacheConfiguration<MetricKey, Metric> historicalCfg = new CacheConfiguration<>(historicalCacheName);
        historicalCfg.setIndexedTypes(MetricKey.class, Metric.class);
        historicalCfg.setCacheMode(CacheMode.LOCAL);
        //historicalCfg.setDataRegionName("Persistent_Region");
        myHistorical = ignite.getOrCreateCache(historicalCfg);
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

    private void ingestData(HashMap<String, InputJson> data) {
        for (Map.Entry<String, InputJson> entry : data.entrySet()) {
                myLatest.put(entry.getKey(), new TimedMetric(entry.getValue().val, entry.getValue().timestamp));
                MetricKey metricTmp = new MetricKey(entry.getValue());
                myHistorical.put(metricTmp, new Metric(entry.getValue().val));
                myMeta.put(new MetaMetricKey(entry.getValue()), new MetaMetric(entry.getValue()));
        }
    }

    private HashMap<String, String> extractLatestData(List<String> search) {
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
        SqlFieldsQuery sql = new SqlFieldsQuery("select metricID, timestamp, val from METRIC WHERE metricID = " + metric + " AND timestamp >= " + min + " AND timestamp <= " + max);
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

    private HashMap<String, String> extractMeta(List<String> search) {
        HashMap<String, String> meta = new HashMap<>();
        for (String metric : search) {
            SqlFieldsQuery sql = new SqlFieldsQuery("select entityID, entityType, name, units, desc, groupName, minVal, maxVal, higherIsBetter from METAMETRIC WHERE metricID = " + metric);
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

    private List<String> getMetricID(List<String> entities) {
        List<String> metrics = new ArrayList<>();
        for (String entity : entities) {
            SqlFieldsQuery sql = new SqlFieldsQuery("select metricID from METAMETRIC WHERE entityID = " + entity);
            try (QueryCursor<List<?>> cursor = myMeta.query(sql)) {
                for (List<?> row : cursor)
                    metrics.add(row.get(0).toString());
            }
        }
        return metrics;
    }


    private class CustomHttpServer extends AbstractHttpServer {

        private final byte[] URI_PUT = "/put".getBytes();

        private final byte[] URI_GET = "/get".getBytes();

        @Override
        protected HttpStatus handle(Channel ctx, Buf buf, RapidoidHelper req) {
            if (!req.isGet.value) {
                if (matches(buf, req.path, URI_PUT)) {
                    //Data structure for metrics
                    HashMap<String, InputJson> metrics = new HashMap<>();
                    //Read and parse json
                    String body = buf.get(req.body);
                    JSONObject obj = new JSONObject(body);
                    //Get the monitoring keyword
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
                    ingestData(metrics);
                    return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("OK", "Ingestion successful!"));
                } else if (matches(buf, req.path, URI_GET)) {
                    HashMap<String, String> result = new HashMap<>();
                    HashMap<String, String> meta = new HashMap<>();
                    //Read and parse json
                    String body = buf.get(req.body);
                    if (!body.equals("")) { //Check if body has filters
                        JSONObject obj = new JSONObject(body);
                        try {
                            ArrayList<String> ids = new ArrayList<>();
                            if (obj.has("entityID")) {
                                JSONArray entities = obj.getJSONArray("entityID");
                                List<String> entitiesList = entities.toList().stream().map(Object::toString).collect(Collectors.toList());
                                List<String> metrics = getMetricID(entitiesList);
                                ids.addAll(metrics);
                            } else if (obj.has("metricID")) {
                                JSONArray entities = obj.getJSONArray("metricID");
                                for (Object ent : entities) { //Store metric ids to list
                                    ids.add(ent.toString());
                                }
                            }
                            if (obj.has("latest") && obj.getBoolean("latest")) { //Get only the latest data
                                result = extractLatestData(ids);
                            } else if (obj.has("from") && obj.has("to")) {
                                long from = obj.getLong("from");
                                long to = obj.getLong("to");
                                for (String metric : ids) {
                                    result.put(metric, extractHistoricalData(metric, from, to));
                                }
                            }
                            //Get metadata
                            meta = extractMeta(ids);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return serializeToJson(HttpUtils.noReq(), ctx, req.isKeepAlive.value, new Message("ERROR", "Error on data extraction!"));
                        }
                    }
                    //Join meta with values
                    List<String> metaValues = new ArrayList<>();
                    for (String metric : result.keySet()) {
                        metaValues.add(" { \"metricID\": " + metric + " , " + result.get(metric) + " , " + meta.getOrDefault(metric, "") + " } ");
                    }
                    String finalRes = "{\"monitoring\": [ " + String.join(", ", metaValues) + " ]} ";
                    return json(ctx, req.isKeepAlive.value, finalRes.getBytes());
                }
            }
            return HttpStatus.NOT_FOUND;
        }
    }

}
