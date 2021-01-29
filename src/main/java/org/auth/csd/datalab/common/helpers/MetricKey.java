package org.auth.csd.datalab.common.helpers;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class MetricKey {

    public MetricKey(InputJson tmp) {
        this.metricID = tmp.metricID;
        this.timestamp = tmp.timestamp;
    }

    public MetricKey(String metricID, long timestamp) {
        this.metricID = metricID;
        this.timestamp = timestamp;
    }

    @QuerySqlField(index = true)
    public String metricID;
    @QuerySqlField(index = true, descending = true)
    public long timestamp;

    @Override
    public String toString() {
        return "{" +
                " \"metricID\": \"" + metricID + "\"" +
                ", \"timestamp\": " + timestamp +
                '}';
    }
}
