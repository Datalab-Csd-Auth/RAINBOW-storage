package org.auth.csd.datalab.common.interfaces;

import org.apache.ignite.services.Service;
import org.auth.csd.datalab.common.models.InputJson;
import org.auth.csd.datalab.common.models.Monitoring;
import org.auth.csd.datalab.common.models.keys.MetricKey;
import org.auth.csd.datalab.common.models.values.MetaMetric;

import java.util.HashMap;
import java.util.HashSet;

public interface MovementInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "MovementService";

    void ingestMonitoring(HashMap<MetricKey, InputJson> metrics);

    HashMap<String, HashMap<MetricKey, Monitoring>> extractMonitoring(HashMap<String, HashSet<String>> filter, Long from, Long to, HashSet<String> nodeList);
    Double extractMonitoringQuery(HashMap<String, HashSet<String>> filter, Long from, Long to, HashSet<String> nodeList, int agg);
    HashMap<MetricKey, MetaMetric> extractMonitoringList(HashMap<String, HashSet<String>> filter);

    HashMap<String, Boolean> extractNodes();

}
