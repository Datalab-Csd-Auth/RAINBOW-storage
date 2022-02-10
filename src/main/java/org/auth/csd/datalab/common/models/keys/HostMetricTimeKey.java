package org.auth.csd.datalab.common.models.keys;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.auth.csd.datalab.common.models.InputJson;

import java.util.Objects;


public class HostMetricTimeKey extends HostMetricKey {

    public HostMetricTimeKey(String metricID, String entityID, String hostname, long timestamp) {
        super(metricID, entityID, hostname);
        this.timestamp = timestamp;
    }

    public HostMetricTimeKey(HostMetricKey metric, long timestamp) {
        super(metric.metric, metric.hostname);
        this.timestamp = timestamp;
    }

    public HostMetricTimeKey(InputJson tmp, String host) {
        super(tmp, host);
        this.timestamp = tmp.timestamp;
    }

    @QuerySqlField(index = true, descending = true)
    public long timestamp;
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HostMetricTimeKey otherKey = (HostMetricTimeKey) o;
        return metric.equals(otherKey.metric) && hostname.equals(otherKey.hostname) && timestamp == otherKey.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(metric, hostname, timestamp);
    }

    @Override
    public String toString() {
        return  "\"node\": \"" + hostname + "\"" +
                ", \"metricID\": \"" + metric.metricID + "\"" +
                ", \"entityID\": \"" + metric.entityID + "\"" +
                ", \"timestamp\": " + timestamp;
    }
}
