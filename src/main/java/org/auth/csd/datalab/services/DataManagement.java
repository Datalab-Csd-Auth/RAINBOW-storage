package org.auth.csd.datalab.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.transactions.Transaction;
import org.auth.csd.datalab.common.Helpers.Tuple2;
import org.auth.csd.datalab.common.interfaces.DataManagementInterface;
import org.auth.csd.datalab.common.models.InputJson;
import org.auth.csd.datalab.common.models.keys.*;
import org.auth.csd.datalab.common.models.values.Metric;
import org.auth.csd.datalab.common.models.values.TimedMetric;

import java.util.*;

import static org.auth.csd.datalab.ServerNodeStartup.*;
import static org.auth.csd.datalab.ServerNodeStartup.APP_CACHE_NAME;
import static org.auth.csd.datalab.common.Helpers.combineTuples;
import static org.auth.csd.datalab.common.Helpers.getQueryValues;

public class DataManagement implements DataManagementInterface {

    @IgniteInstanceResource
    private static Ignite ignite;
    /**
     * Reference to the cache.
     */
    private static IgniteCache<HostMetricKey, TimedMetric> myLatest;
    private static IgniteCache<HostMetricTimeKey, Metric> myHistorical;
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
    public boolean deleteApp(Set<String> ids) {
        boolean result = false;
        if(!ids.isEmpty()) {
            String sql = "DELETE FROM METRIC WHERE key IN ('" + String.join("','", ids) + "')";
            // Iterate over the result set.
            try (QueryCursor<List<?>> cursor = getQueryValues(myApp, sql)) {
                for (List<?> row : cursor) {
                    result = ((Long) row.get(0) != 0);
                    if(result) break;
                }
            }
        }
        return result;
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
    public boolean deleteAnalytics(Set<String> ids) {
        boolean result = false;
        if (!ids.isEmpty()) {
            String sql = "DELETE FROM METRIC WHERE key IN ('" + String.join("','", ids) + "')";
            // Iterate over the result set.
            try (QueryCursor<List<?>> cursor = getQueryValues(myAnalytics, sql)) {
                for (List<?> row : cursor) {
                    result = ((Long) row.get(0) != 0);
                    if(result) break;
                }
            }
        }
        return result;
    }

    //------------MONITORING----------------
    private List<TimedMetric> extractMonitoringData(HostMetricKey key) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT timestamp, val " +
                "FROM TIMEDMETRIC " +
                "WHERE metricID = '" + key.metric.metricID + "' AND entityID = '" + key.metric.entityID + "' AND hostname = '" + key.hostname + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myLatest, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double) row.get(1), (long) row.get(0)));
        }
        //If main memory cache is empty (due to restart)
        if (result.isEmpty()) {
            sql = "SELECT a.timestamp, a.val " +
                    "FROM METRIC AS a " +
                    "INNER JOIN (SELECT metricID, entityID, hostname, MAX(timestamp) as timestamp FROM METRIC GROUP BY (metricID, entityID, hostname)) AS b " +
                    "ON a.metricID = b.metricID AND a.timestamp = b.timestamp AND a.entityID = b.entityID AND a.hostname = b.hostname " +
                    "WHERE a.metricID = '" + key.metric.metricID + "' AND a.entityID = '" + key.metric.entityID + "' AND a.hostname = '" + key.hostname + "'";
            // Iterate over the result set.
            try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, sql)) {
                for (List<?> row : cursor)
                    result.add(new TimedMetric((Double) row.get(1), (long) row.get(0)));
            }
        }
        return result;
    }

    private List<TimedMetric> extractMonitoringData(HostMetricKey key, long from) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT timestamp, val " +
                "FROM METRIC " +
                "WHERE metricID = '" + key.metric.metricID + "' AND entityID = '" + key.metric.entityID + "' AND hostname = '" + key.hostname + "' AND timestamp >= " + from;
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double) row.get(1), (long) row.get(0)));
        }
        return result;
    }

    private List<TimedMetric> extractMonitoringData(HostMetricKey key, long from, long to) {
        List<TimedMetric> result = new ArrayList<>();
        String sql = "SELECT timestamp, val " +
                "FROM METRIC " +
                "WHERE metricID = '" + key.metric.metricID + "' AND entityID = '" + key.metric.entityID + "' AND hostname = '" + key.hostname + "' AND timestamp BETWEEN " + from + " AND " + to;
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, sql)) {
            for (List<?> row : cursor)
                result.add(new TimedMetric((Double) row.get(1), (long) row.get(0)));
        }
        return result;
    }

    private Tuple2<Double, Long> extractMonitoringQuery(HostMetricKey key) {
        Tuple2<Double, Long> res = null;
        String sql = "SELECT val " +
                "FROM TIMEDMETRIC " +
                "WHERE metricID = '" + key.metric.metricID + "' AND entityID = '" + key.metric.entityID + "' AND hostname = '" + key.hostname + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myLatest, sql)) {
            for (List<?> row : cursor)
                res = new Tuple2<>((Double) row.get(0), 1L);
        }
        //If main memory cache is empty (due to restart){
        sql = "SELECT a.val " +
                "FROM METRIC AS a " +
                "INNER JOIN (SELECT metricID, entityID, hostname, MAX(timestamp) as timestamp FROM METRIC GROUP BY (metricID, entityID, hostname)) AS b " +
                "ON a.metricID = b.metricID AND a.timestamp = b.timestamp AND a.entityID = b.entityID AND a.hostname = b.hostname " +
                "WHERE a.metricID = '" + key.metric.metricID + "' AND a.entityID = '" + key.metric.entityID + "' AND a.hostname = '" + key.hostname + "'";
        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = getQueryValues(myHistorical, sql)) {
            for (List<?> row : cursor)
                res = new Tuple2<>((Double) row.get(0), 1L);
        }
        return res;
    }

    private Tuple2<Double, Long> extractMonitoringQuery(HostMetricKey key, long from, int agg) {
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
        String rest = "FROM METRIC " +
                "WHERE metricID = '" + key.metric.metricID + "' AND entityID = '" + key.metric.entityID + "' AND hostname = '" + key.hostname + "' AND timestamp >= " + from;
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

    private Tuple2<Double, Long> extractMonitoringQuery(HostMetricKey key, long from, long to, int agg) {
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
        String rest = "FROM METRIC " +
                "WHERE metricID = '" + key.metric.metricID + "' AND entityID = '" + key.metric.entityID + "' AND hostname = '" + key.hostname + "' AND timestamp BETWEEN " + from + " AND " + to;
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
    public void ingestHistoricalMonitoring(HashMap<HostMetricKey, List<TimedMetric>> values) {
        for (Map.Entry<HostMetricKey, List<TimedMetric>> entry : values.entrySet()) {
            HostMetricKey host = entry.getKey();
            HashMap<HostMetricTimeKey, Metric> tmp = new HashMap<>();
            entry.getValue().forEach(k -> tmp.put(new HostMetricTimeKey(host, k.timestamp), new Metric(k.val)));
            myHistorical.putAll(tmp);
        }
    }

    @Override
    //Ingest historical monitoring data
    public void ingestMonitoring(HashMap<MetricKey, InputJson> metrics, String hostname) {
        IgniteTransactions transactions = ignite.transactions();
        for (Map.Entry<MetricKey, InputJson> entry : metrics.entrySet()) {
            myLatest.put(new HostMetricKey(entry.getKey(),hostname), new TimedMetric(entry.getValue().val, entry.getValue().timestamp));
            try (Transaction tx = transactions.txStart()) {
                myHistorical.put(new HostMetricTimeKey(entry.getValue(), hostname), new Metric(entry.getValue().val));
                tx.commit();
            }
        }
    }

    @Override
    //Extract monitoring data
    public HashMap<HostMetricKey, List<TimedMetric>> extractMonitoring(Set<HostMetricKey> keys, Long from, Long to) {
        HashMap<HostMetricKey, List<TimedMetric>> result = new HashMap<>();
        for (HostMetricKey myKey : keys) {
            List<TimedMetric> data;
            if (from < 0) data = extractMonitoringData(myKey);
            else if (to >= from) data = extractMonitoringData(myKey, from, to);
            else data = extractMonitoringData(myKey, from);
            result.put(myKey, data);
        }
        return result;
    }

    @Override
    //Extract monitoring data with aggregation
    public Tuple2<Double, Long> extractMonitoringQuery(Set<HostMetricKey> keys, Long from, Long to, int agg) {
        Double tmpVal = (agg == 1) ? Double.MAX_VALUE : 0L;
        Tuple2<Double, Long> result = new Tuple2<>(tmpVal, 0L);
        for (HostMetricKey myKey : keys) {
            Tuple2<Double, Long> tmp;
            if (from < 0) tmp = extractMonitoringQuery(myKey);
            else if (to >= from) tmp = extractMonitoringQuery(myKey, from, to, agg);
            else tmp = extractMonitoringQuery(myKey, from, agg);
            if(tmp != null){
                result = combineTuples(result, tmp, agg);
            }
        }
        return result;
    }

    @Override
    //Delete monitoring data
    public boolean deleteMonitoring(Set<HostMetricKey> keys) {
        if (!keys.isEmpty()) {
            StringBuilder sql = new StringBuilder(" WHERE ");
            keys.forEach((k -> sql.append(" (metricID = '").append(k.metric.metricID)
                    .append("' AND entityID = '").append(k.metric.entityID)
                    .append("' AND hostname = '").append(k.hostname).append("') OR")));
            sql.delete(sql.length() - 2, sql.length());
            getQueryValues(myHistorical, "DELETE FROM METRIC " + sql);
            getQueryValues(myLatest, "DELETE FROM TIMEDMETRIC " + sql);
            return true;
        } else return false;
    }

    //-------------------Ignite service functions-------------------
    @Override
    public void init(ServiceContext ctx) {
        //Get the cache that is designed in the config for the latest data
        myLatest = ignite.cache(LATEST_CACHE_NAME);
        myHistorical = ignite.cache(HISTORICAL_CACHE_NAME);
        myAnalytics = ignite.cache(ANALYTICS_CACHE_NAME);
        if (APP_CACHE) myApp = ignite.cache(APP_CACHE_NAME);
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
