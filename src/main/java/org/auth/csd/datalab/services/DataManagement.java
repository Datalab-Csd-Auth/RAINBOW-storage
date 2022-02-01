package org.auth.csd.datalab.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.auth.csd.datalab.common.Helpers.Tuple2;
import org.auth.csd.datalab.common.interfaces.DataManagementInterface;
import org.auth.csd.datalab.common.models.InputJson;
import org.auth.csd.datalab.common.models.Monitoring;
import org.auth.csd.datalab.common.models.keys.AnalyticKey;
import org.auth.csd.datalab.common.models.keys.MetricKey;
import org.auth.csd.datalab.common.models.keys.MetricTimeKey;
import org.auth.csd.datalab.common.models.values.MetaMetric;
import org.auth.csd.datalab.common.models.values.Metric;
import org.auth.csd.datalab.common.models.values.TimedMetric;

import java.util.*;
import java.util.stream.Collectors;

import static org.auth.csd.datalab.ServerNodeStartup.*;
import static org.auth.csd.datalab.ServerNodeStartup.appCacheName;
import static org.auth.csd.datalab.common.Helpers.combineTuples;
import static org.auth.csd.datalab.common.Helpers.getQueryValues;

public class DataManagement implements DataManagementInterface {

    @IgniteInstanceResource
    private static Ignite ignite;
    /**
     * Reference to the cache.
     */
    private static IgniteCache<MetricKey, TimedMetric> myLatest;
    private static IgniteCache<MetricTimeKey, Metric> myHistorical;
    private static IgniteCache<MetricKey, MetaMetric> myMeta;
    private static IgniteCache<AnalyticKey, Metric> myAnalytics;
    private static IgniteCache<AnalyticKey, Metric> myApp = null;

    //------------APP----------------
    private List<String> extractAppKeys() {
        List<String> result = new ArrayList<>();
        String sql = "SELECT key FROM METRIC";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myApp, sql)) {
            for (List<?> row : cursor) {
                result.add(row.get(0).toString());
            }
        }
        return result;
    }

    private List<TimedMetric> extractAppData(String key) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT a.* " +
                "FROM METRIC AS a " +
                "INNER JOIN (SELECT key, MAX(timestamp) as timestamp FROM METRIC GROUP BY key) AS b ON a.key = b.key AND a.timestamp = b.timestamp " +
                "WHERE a.key = '" + key + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myApp, sql)) {
            for (List<?> row : cursor) {
                result.add(new TimedMetric((Double) row.get(2), (long) row.get(1)));
            }
        }
        return result;
    }

    private List<TimedMetric> extractAppData(String key, long from) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT key, timestamp, val FROM METRIC WHERE timestamp >= " + from + " AND key = '" + key + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myApp, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double) row.get(2), (long) row.get(1)));
        }
        return result;
    }

    private List<TimedMetric> extractAppData(String key, long from, long to) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT key, timestamp, val FROM METRIC WHERE timestamp BETWEEN " + from + " AND " + to + " AND key = '" + key + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myApp, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double) row.get(2), (long) row.get(1)));
        }
        return result;
    }

    @Override
    //Ingest new application data
    public void ingestApp(HashMap<AnalyticKey, Metric> data) {
        if (myApp != null)
            for (Map.Entry<AnalyticKey, Metric> entry : data.entrySet()) {
                myApp.put(entry.getKey(), entry.getValue());
            }
    }

    @Override
    //Extract historical analytics data
    public HashMap<String, List<TimedMetric>> extractApp(Set<String> filter, long from, long to) {
        HashMap<String, List<TimedMetric>> result = new HashMap<>();
        if (filter.isEmpty()) {
            filter.addAll(extractAppKeys());
        }
        for (String key : filter) {
            List<TimedMetric> data;
            if (from < 0) data = extractAppData(key);
            else if (to >= from) data = extractAppData(key, from, to);
            else data = extractAppData(key, from);
            if (!data.isEmpty()) result.put(key, data);
        }
        return result;
    }

    @Override
    //Delete analytics data
    public Boolean deleteApp(Set<String> ids) {
        if(!ids.isEmpty()) {
            String sql = "DELETE FROM METRIC WHERE key IN ('" + String.join("','", ids) + "')";
            // Iterate over the result set.
            try (QueryCursor<List<?>> cursor = getQueryValues(myApp, sql)) {
                for (List<?> row : cursor) {
                    return !((Long) row.get(0) == 0);
                }
            }
            return false;
        }
        return false;
    }

    //------------ANALYTICS----------------
    private List<String> extractAnalyticsKeys() {
        List<String> result = new ArrayList<>();
        String sql = "SELECT key FROM METRIC";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myAnalytics, sql)) {
            for (List<?> row : cursor) {
                result.add(row.get(0).toString());
            }
        }
        return result;
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

    private List<TimedMetric> extractAnalyticsData(String key, long from) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT key, timestamp, val FROM METRIC WHERE timestamp >= " + from + " AND key = '" + key + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myAnalytics, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double) row.get(2), (long) row.get(1)));
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

    @Override
    //Ingest new analytics data
    public void ingestAnalytics(HashMap<AnalyticKey, Metric> data) {
        for (Map.Entry<AnalyticKey, Metric> entry : data.entrySet()) {
            myAnalytics.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    //Extract historical analytics data
    public HashMap<String, List<TimedMetric>> extractAnalytics(Set<String> filter, long from, long to) {
        HashMap<String, List<TimedMetric>> result = new HashMap<>();
        if (filter.isEmpty()) {
            filter.addAll(extractAnalyticsKeys());
        }
        for (String key : filter) {
            List<TimedMetric> data;
            if (from < 0) data = extractAnalyticsData(key);
            else if (to >= from) data = extractAnalyticsData(key, from, to);
            else data = extractAnalyticsData(key, from);
            if (!data.isEmpty()) result.put(key, data);
        }
        return result;
    }

    @Override
    //Delete analytics data
    public Boolean deleteAnalytics(Set<String> ids) {
        if (!ids.isEmpty()) {
            String sql = "DELETE FROM METRIC WHERE key IN ('" + String.join("','", ids) + "')";
            // Iterate over the result set.
            try (QueryCursor<List<?>> cursor = getQueryValues(myAnalytics, sql)) {
                for (List<?> row : cursor) {
                    return !((Long) row.get(0) == 0);
                }
            }
            return false;
        }
        return false;
    }

    //------------MONITORING----------------
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

    private List<TimedMetric> extractMonitoringData(MetricKey key, long from) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT timestamp, val FROM METRIC WHERE metricID = '" + key.metricID + "' AND entityID = '" + key.entityID + "' AND timestamp >= " + from;
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double) row.get(1), (long) row.get(0)));
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

    private Tuple2<Double, Long> extractMonitoringData(MetricKey key, int agg) {
        String select = "SELECT val ";
        String rest = "FROM TIMEDMETRIC WHERE metricID = '" + key.metricID + "' AND entityID = '" + key.entityID + "' ";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myLatest, select + rest)) {
            for (List<?> row : cursor)
                return new Tuple2<>((Double) row.get(0), 1L);
        }
        //If main memory cache is empty (due to restart){
        String sql = "SELECT a.val " +
                "FROM METRIC AS a " +
                "INNER JOIN (SELECT metricID, entityID, MAX(timestamp) as timestamp FROM METRIC GROUP BY (metricID, entityID)) AS b ON a.metricID = b.metricID AND a.timestamp = b.timestamp AND a.entityID = b.entityID " +
                "WHERE a.metricID = '" + key.metricID + "' AND a.entityID = '" + key.entityID + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, sql)) {
            for (List<?> row : cursor)
                return new Tuple2<>((Double) row.get(0), 1L);
        }
        return null;
    }

    private Tuple2<Double, Long> extractMonitoringData(MetricKey key, long from, int agg) {
        String select = "SELECT ";
        switch (agg) {
            case 0:
                select += "MAX(val) ";
                break;
            case 1:
                select += "MIN(val) ";
                break;
            case 2:
                select += "SUM(val) ";
                break;
            case 3:
                select += "SUM(val), COUNT(val) ";
                break;
        }
        String rest = "FROM METRIC WHERE metricID = '" + key.metricID + "' AND entityID = '" + key.entityID + "' AND timestamp >= " + from + " ";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, select + rest)) {
            for (List<?> row : cursor)
                if (agg == 3)
                    return new Tuple2<>((Double) row.get(0), (Long) row.get(1));
                else
                    return new Tuple2<>((Double) row.get(0), 1L);
        }
        return null;
    }

    private Tuple2<Double, Long> extractMonitoringData(MetricKey key, long from, long to, int agg) {
        String select = "SELECT ";
        switch (agg) {
            case 0:
                select += "MAX(val) ";
                break;
            case 1:
                select += "MIN(val) ";
                break;
            case 2:
                select += "SUM(val) ";
                break;
            case 3:
                select += "SUM(val), COUNT(val) ";
                break;
        }
        String rest = "FROM METRIC WHERE metricID = '" + key.metricID + "' AND entityID = '" + key.entityID + "' AND timestamp BETWEEN " + from + " AND " + to;
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, select + rest)) {
            for (List<?> row : cursor)
                if (agg == 3)
                    return new Tuple2<>((Double) row.get(0), (Long) row.get(1));
                else
                    return new Tuple2<>((Double) row.get(0), 1L);
        }
        return null;
    }

    @Override
    //Ingest new monitoring data
    public void ingestMonitoring(HashMap<MetricKey, InputJson> metrics) {
        for (Map.Entry<MetricKey, InputJson> entry : metrics.entrySet()) {
            myMeta.put(entry.getKey(), new MetaMetric(entry.getValue()));
            myLatest.put(entry.getKey(), new TimedMetric(entry.getValue().val, entry.getValue().timestamp));
            myHistorical.put(new MetricTimeKey(entry.getValue()), new Metric(entry.getValue().val));
        }
    }

    @Override
    //Extract only meta data
    public HashMap<MetricKey, MetaMetric> extractMeta(HashMap<String, HashSet<String>> filter) {
        return extractMetaData(filter);
    }

    @Override
    //Extract monitoring data
    public HashMap<MetricKey, Monitoring> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to) {
        HashMap<MetricKey, Monitoring> result = new HashMap<>();
        HashMap<MetricKey, MetaMetric> metaValues = extractMetaData(filter);
        for (MetricKey myKey : metaValues.keySet()) {
            List<TimedMetric> data;
            if (from < 0) data = extractMonitoringData(myKey);
            else if (to >= from) data = extractMonitoringData(myKey, from, to);
            else data = extractMonitoringData(myKey, from);
            result.put(myKey, new Monitoring(metaValues.get(myKey), data));
        }
        return result;
    }

    @Override
    //Extract monitoring data with aggregation
    public Tuple2<Double, Long> extractMonitoringSingle(HashMap<String, HashSet<String>> filter, Long from, Long to, int agg) {
        Double tmpVal = (agg == 1) ? Double.MAX_VALUE : 0L;
        Tuple2<Double, Long> result = new Tuple2<>(tmpVal, 0L);
        HashMap<MetricKey, MetaMetric> metaValues = extractMetaData(filter);
        for (MetricKey myKey : metaValues.keySet()) {
            Tuple2<Double, Long> tmp;
            if (from < 0) tmp = extractMonitoringData(myKey, agg);
            else if (to >= from) tmp = extractMonitoringData(myKey, from, to, agg);
            else tmp = extractMonitoringData(myKey, from, agg);
            if(tmp != null){
                result = combineTuples(result, tmp, agg);
            }
        }
        return result;
    }

    @Override
    //Delete monitoring data
    public Boolean deleteMonigoring(HashMap<String, HashSet<String>> filter) {
        HashMap<MetricKey, MetaMetric> metaValues = extractMetaData(filter);
        if (!metaValues.isEmpty()) {
            StringBuilder sql = new StringBuilder(" WHERE ");
            metaValues.keySet().forEach((k -> sql.append(" (metricID = '").append(k.metricID).append("' AND entityID = '").append(k.entityID).append("') OR")));
            sql.delete(sql.length() - 2, sql.length());
            getQueryValues(myHistorical, "DELETE FROM METRIC " + sql);
            getQueryValues(myLatest, "DELETE FROM TIMEDMETRIC " + sql);
            getQueryValues(myMeta, "DELETE FROM METAMETRIC " + sql);
            return true;
        } else return false;
    }

    //-------------------Ignite service functions-------------------
    @Override
    public void init(ServiceContext ctx) {
        //Get the cache that is designed in the config for the latest data
        myLatest = ignite.cache(latestCacheName);
        myHistorical = ignite.cache(historicalCacheName);
        myMeta = ignite.cache(metaCacheName);
        myAnalytics = ignite.cache(analyticsCacheName);
        if (app_cache) myApp = ignite.cache(appCacheName);
        System.out.println("Initializing Data Management on node:" + ignite.cluster().localNode());
    }

    @Override
    public void execute(ServiceContext ctx) {
        System.out.println("Executing Data Management on node:" + ignite.cluster().localNode());
    }

    @Override
    public void cancel(ServiceContext ctx) {
        System.out.println("Stopping Data Management on node:" + ignite.cluster().localNode());
    }

}
