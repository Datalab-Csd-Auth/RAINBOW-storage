package org.auth.csd.datalab.common.models.keys;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.auth.csd.datalab.common.models.InputJson;

public class MetricTimeKey extends MetricKey {

    public MetricTimeKey(InputJson tmp) {
        super(tmp);
        this.timestamp = tmp.timestamp;
    }

    public MetricTimeKey(String metricID, String entityID, long timestamp) {
        super(metricID, entityID);
        this.timestamp = timestamp;
    }

    public MetricTimeKey(MetricKey metric, long timestamp) {
        super(metric.metricID, metric.entityID);
        this.timestamp = timestamp;
    }

    @QuerySqlField(index = true, descending = true)
    public long timestamp;

    @Override
    public String toString() {
        return  "\"metricID\": \"" + metricID + "\"" +
                ", \"entityID\": \"" + entityID + "\"" +
                ", \"timestamp\": " + timestamp;
    }
}
