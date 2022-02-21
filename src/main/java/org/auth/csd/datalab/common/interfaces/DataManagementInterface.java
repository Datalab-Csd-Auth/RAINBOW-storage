package org.auth.csd.datalab.common.interfaces;

import org.apache.ignite.services.Service;
import org.auth.csd.datalab.common.models.InputJson;
import org.auth.csd.datalab.common.models.keys.AnalyticKey;
import org.auth.csd.datalab.common.models.keys.HostMetricKey;
import org.auth.csd.datalab.common.models.keys.MetricKey;
import org.auth.csd.datalab.common.models.values.Metric;
import org.auth.csd.datalab.common.models.values.TimedMetric;
import org.auth.csd.datalab.common.Helpers.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

public interface DataManagementInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "DataManagementService";

    void ingestMonitoring(HashMap<MetricKey, InputJson> metrics, String hostname);
    void ingestHistoricalMonitoring(HashMap<HostMetricKey, List<TimedMetric>> values);
    HashMap<HostMetricKey, List<TimedMetric>> extractMonitoring(Set<HostMetricKey> keys, Long from, Long to);
    Tuple2<Double,Long> extractMonitoringQuery(Set<HostMetricKey> keys, Long from, Long to, int agg);
    boolean deleteMonitoring(Set<HostMetricKey> keys);

    void ingestAnalytics(HashMap<AnalyticKey, Metric> data);
    HashMap<String, List<TimedMetric>> extractAnalytics(Set<String> ids, long from, long to);
    boolean deleteAnalytics(Set<String> ids);

    void ingestApp(HashMap<AnalyticKey, Metric> data);
    HashMap<String, List<TimedMetric>> extractApp(Set<String> ids, long from, long to);
    boolean deleteApp(Set<String> ids);

}
