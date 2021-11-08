package org.auth.csd.datalab.common.interfaces;

import org.apache.ignite.services.Service;

import java.util.*;

public interface DataInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "DataService";

    ArrayList<String> extractMonitoring(HashMap<String, HashSet<String>> ids);

    ArrayList<String> extractMonitoring(HashMap<String, HashSet<String>> ids, Long from, Long to);

    HashMap<String, String> extractAnalytics(Set<String> ids);

}
