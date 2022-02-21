package org.auth.csd.datalab.common.interfaces;

import org.apache.ignite.services.Service;
import org.auth.csd.datalab.common.models.InputJson;
import org.auth.csd.datalab.common.models.Monitoring;
import org.auth.csd.datalab.common.models.keys.HostMetricKey;
import org.auth.csd.datalab.common.models.keys.MetricKey;
import org.auth.csd.datalab.common.models.values.MetaMetric;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public interface MovementInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "MovementService";

    void ingestMonitoring(HashMap<MetricKey, InputJson> metrics);
    HashMap<String, HashMap<MetricKey, Monitoring>> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to, HashSet<String> nodeList);
    Double extractMonitoringQuery(HashMap<String, HashSet<String>> filter, Long from, Long to, HashSet<String> nodeList, int agg);
    HashMap<HostMetricKey, MetaMetric> extractMonitoringList(HashMap<String, HashSet<String>> filter, HashSet<String> nodesList);
    boolean deleteMonitoring(HashMap<String, HashSet<String>> filter, HashSet<String> nodeList);

    HashMap<String, Boolean> extractNodes();

    void startReplication(Set<HostMetricKey> metrics, String remote);
    void setReplication(boolean replica);

}
