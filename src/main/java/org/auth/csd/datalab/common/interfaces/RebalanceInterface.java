package org.auth.csd.datalab.common.interfaces;

import org.apache.ignite.services.Service;

import java.util.Set;

public interface RebalanceInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "RebalanceService";

//    public Void rebalanceData(Set<String> keys);
}
