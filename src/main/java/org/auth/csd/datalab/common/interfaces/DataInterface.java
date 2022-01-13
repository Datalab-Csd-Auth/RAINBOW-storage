package org.auth.csd.datalab.common.interfaces;

import org.apache.ignite.services.Service;
import org.auth.csd.datalab.common.models.Monitoring;
import org.auth.csd.datalab.common.models.keys.MetricKey;
import org.auth.csd.datalab.common.models.values.TimedMetric;

import java.util.*;

public interface DataInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "DataService";

    HashMap<MetricKey, Monitoring> extractMonitoring(HashMap<String, HashSet<String>> filter);
    HashMap<MetricKey, Monitoring> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to);
    HashMap<MetricKey, Monitoring> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to, String agg);
    String extractMonitoringJson(HashMap<String, HashSet<String>> filter);
    String extractMonitoringJson(HashMap<String, HashSet<String>> filter, Long from, Long to);

    HashMap<String, List<TimedMetric>> extractAnalytics(Set<String> ids);
    HashMap<String, List<TimedMetric>> extractAnalytics(Set<String> ids, long from, long to);
    String extractAnalyticsJson(Set<String> ids);
    String extractAnalyticsJson(Set<String> ids, long from, long to);

}
