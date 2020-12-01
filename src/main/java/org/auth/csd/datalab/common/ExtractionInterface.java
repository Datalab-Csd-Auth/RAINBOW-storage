package org.auth.csd.datalab.common;

import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.services.Service;

import java.util.HashMap;

public interface ExtractionInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "ExtractionService";

    public HashMap<String, Object> extractData(IgniteBiPredicate<String, Object> filter, boolean remove);

}
