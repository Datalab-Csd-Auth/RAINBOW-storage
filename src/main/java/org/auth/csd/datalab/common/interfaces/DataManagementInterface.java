package org.auth.csd.datalab.common.interfaces;

import org.apache.ignite.services.Service;
import org.auth.csd.datalab.common.models.InputJson;
import org.auth.csd.datalab.common.models.Monitoring;
import org.auth.csd.datalab.common.models.keys.AnalyticKey;
import org.auth.csd.datalab.common.models.keys.MetricKey;
import org.auth.csd.datalab.common.models.values.MetaMetric;
import org.auth.csd.datalab.common.models.values.Metric;
import org.auth.csd.datalab.common.models.values.TimedMetric;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public interface DataManagementInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "DataManagementService";

    void ingestMonitoring(HashMap<MetricKey, InputJson> metrics);
    HashMap<MetricKey, MetaMetric> extractMeta(HashMap<String, HashSet<String>> filter);
    HashMap<MetricKey, Monitoring> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to);
    HashMap<MetricKey, Monitoring> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to, int agg);
    Boolean deleteMonigoring(HashMap<String, HashSet<String>> filter);

    void ingestAnalytics(HashMap<AnalyticKey, Metric> data);
    HashMap<String, List<TimedMetric>> extractAnalytics(Set<String> ids, long from, long to);
    Boolean deleteAnalytics(Set<String> ids);

    void ingestApp(HashMap<AnalyticKey, Metric> data);
    HashMap<String, List<TimedMetric>> extractApp(Set<String> ids, long from, long to);
    Boolean deleteApp(Set<String> ids);

}
