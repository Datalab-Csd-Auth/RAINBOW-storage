package org.auth.csd.datalab.common.interfaces;

import org.apache.ignite.services.Service;
import org.auth.csd.datalab.common.helpers.Metric;
import org.auth.csd.datalab.common.helpers.MetricKey;

import java.util.HashMap;
import java.util.List;

public interface DataInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "DataService";

//    public void ingestData(HashMap<String, InputJson> data);

    HashMap<String, String> extractLatestData(List<String> search);

    String extractHistoricalData(String metric, Long min, Long max);

}
