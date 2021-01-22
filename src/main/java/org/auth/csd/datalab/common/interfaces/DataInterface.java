package org.auth.csd.datalab.common.interfaces;

import org.apache.ignite.services.Service;
import org.auth.csd.datalab.common.helpers.Metric;

import java.util.HashMap;
import java.util.HashSet;

public interface DataInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "IngestionService";

    public void ingestData(HashMap<String, Metric> data);

    public HashSet<String> extractData(HashSet<String> search);
}
