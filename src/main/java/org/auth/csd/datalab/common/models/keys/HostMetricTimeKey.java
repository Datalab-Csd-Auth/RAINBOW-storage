package org.auth.csd.datalab.common.models.keys;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.auth.csd.datalab.common.models.InputJson;


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
    public String toString() {
        return  "\"node\": \"" + hostname + "\"" +
                ", \"metricID\": \"" + metric.metricID + "\"" +
                ", \"entityID\": \"" + metric.entityID + "\"" +
                ", \"timestamp\": " + timestamp;
    }
}
