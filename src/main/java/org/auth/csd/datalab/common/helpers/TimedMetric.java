package org.auth.csd.datalab.common.helpers;

public class TimedMetric {

    public TimedMetric(Double val, Long timestamp) {
        this.val = val;
        this.timestamp = timestamp;
    }

    public Double val;
    public Long timestamp;

    @Override
    public String toString() {
        return "{" +
                " \"value\": \"" + val + "\"" +
                ", \"timestamp\": \"" + timestamp + "\"" +
                '}';
    }
}
