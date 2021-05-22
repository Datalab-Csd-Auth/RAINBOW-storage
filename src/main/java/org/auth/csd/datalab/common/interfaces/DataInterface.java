package org.auth.csd.datalab.common.interfaces;

import org.apache.ignite.services.Service;

import java.util.*;

public interface DataInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "DataService";

    HashMap<String, String> extractMonitoring(Set<String> ids, boolean entity);

    HashMap<String, String> extractMonitoring(Set<String> ids, boolean entity, Long from, Long to);

}
