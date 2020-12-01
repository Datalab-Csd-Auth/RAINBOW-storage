package org.auth.csd.datalab.common;

import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.services.Service;

import java.util.HashMap;
import java.util.HashSet;

public interface ClientExtractionInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "ClientExtractionService";

    public HashMap<String, Object> extractMeta(HashSet<String> queriedNodes);

    public HashMap<String, Object> extractData(HashMap<String, HashSet<String>> data);


}
