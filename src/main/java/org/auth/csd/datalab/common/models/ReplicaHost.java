package org.auth.csd.datalab.common.models;

import org.auth.csd.datalab.common.models.keys.MetricKey;

import java.util.HashSet;

public class ReplicaHost extends Replica{

    public String host;

    public ReplicaHost(String host) {
        super();
        this.host = host;
    }

    public ReplicaHost(String host, HashSet<MetricKey> keys) {
        super(keys);
        this.host = host;
    }
}
