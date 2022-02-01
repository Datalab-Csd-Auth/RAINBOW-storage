package org.auth.csd.datalab.common.interfaces;

import org.apache.ignite.services.Service;
import org.auth.csd.datalab.common.models.Monitoring;
import org.auth.csd.datalab.common.models.keys.MetricKey;
import org.auth.csd.datalab.common.models.values.MetaMetric;
import org.auth.csd.datalab.common.models.values.TimedMetric;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public interface HttpInterface extends Service {
    /** Service name */
    public static final String SERVICE_NAME = "HttpService";


}
