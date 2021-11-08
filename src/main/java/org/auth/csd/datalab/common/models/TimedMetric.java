package org.auth.csd.datalab.common.models;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class TimedMetric extends Metric {

    public TimedMetric(Double val, Long timestamp) {
        super(val);
        this.timestamp = timestamp;
    }

    @QuerySqlField
    public Long timestamp;

    @Override
    public String toString() {
        return "{" +
                " \"value\": \"" + val + "\"" +
                ", \"timestamp\": \"" + timestamp + "\"" +
                '}';
    }
}
