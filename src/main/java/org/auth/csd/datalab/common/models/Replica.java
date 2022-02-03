package org.auth.csd.datalab.common.models;

import org.auth.csd.datalab.common.models.keys.MetricKey;

import java.util.HashSet;

public class Replica {

    public HashSet<MetricKey> keys;

    public Replica() {
        this.keys = new HashSet<>();
    }

    public Replica(HashSet<MetricKey> keys) {
        this.keys = keys;
    }
}
