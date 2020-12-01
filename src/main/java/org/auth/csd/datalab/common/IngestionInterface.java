package org.auth.csd.datalab.common;

import org.apache.ignite.services.Service;

import java.util.HashMap;

public interface IngestionInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "IngestionService";

    public void ingestData(HashMap<String, Object> data);
}
