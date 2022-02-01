package org.auth.csd.datalab.common.models.keys;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.util.Objects;
import java.util.UUID;

public class ReplicaMetricKey extends MetricKey {

    public ReplicaMetricKey(String metricID, String entityID, String hostname) {
        super(metricID, entityID);
        this.hostname = hostname;
    }

    public ReplicaMetricKey(MetricKey metric, String hostname) {
        super(metric.metricID, metric.entityID);
        this.hostname = hostname;
    }

    @QuerySqlField(index = true)
    public String hostname;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaMetricKey otherKey = (ReplicaMetricKey) o;
        return metricID.equals(otherKey.metricID) && entityID.equals(otherKey.entityID) && hostname.equals(otherKey.hostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricID, entityID, hostname);
    }

    @Override
    public String toString() {
        return  "\"node\": \"" + hostname + "\"" +
                ", \"metricID\": \"" + metricID + "\"" +
                ", \"entityID\": \"" + entityID + "\"";
    }

}
