package org.auth.csd.datalab.common.models.keys;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.auth.csd.datalab.common.models.InputJson;

import java.util.Objects;

public class HostMetricKey {

    public HostMetricKey(String metricID, String entityID, String hostname) {
        this.metric = new MetricKey(metricID, entityID);
        this.hostname = hostname;
    }

    public HostMetricKey(MetricKey metric, String hostname) {
        this.metric = metric;
        this.hostname = hostname;
    }

    public HostMetricKey(InputJson tmp, String host) {
        this.metric = new MetricKey(tmp);
        this.hostname = host;
    }

    @QuerySqlField(index = true)
    public String hostname;
    @QuerySqlField(index = true)
    public MetricKey metric;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HostMetricKey otherKey = (HostMetricKey) o;
        return metric.equals(otherKey.metric) && hostname.equals(otherKey.hostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metric, hostname);
    }

    @Override
    public String toString() {
        return  "\"node\": \"" + hostname + "\"" +
                ", \"metricID\": \"" + metric.metricID + "\"" +
                ", \"entityID\": \"" + metric.entityID + "\"";
    }

}
