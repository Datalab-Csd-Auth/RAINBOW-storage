package org.auth.csd.datalab.common.models.keys;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.auth.csd.datalab.common.models.InputJson;

import java.util.UUID;

public class ReplicaMetricTimeKey extends ReplicaMetricKey {

    public ReplicaMetricTimeKey(String metricID, String entityID, String hostname, long timestamp) {
        super(metricID, entityID, hostname);
        this.timestamp = timestamp;
    }

    public ReplicaMetricTimeKey(ReplicaMetricKey metric, long timestamp) {
        super(metric.metricID, metric.entityID, metric.hostname);
        this.timestamp = timestamp;
    }

    @QuerySqlField(index = true, descending = true)
    public long timestamp;

    @Override
    public String toString() {
        return  "\"node\": \"" + hostname + "\"" +
                ", \"metricID\": \"" + metricID + "\"" +
                ", \"entityID\": \"" + entityID + "\"" +
                ", \"timestamp\": " + timestamp;
    }
}
