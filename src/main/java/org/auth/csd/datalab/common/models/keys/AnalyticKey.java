package org.auth.csd.datalab.common.models.keys;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class AnalyticKey {

    public AnalyticKey(String key, long timestamp) {
        this.key = key;
        this.timestamp = timestamp;
    }

    @QuerySqlField(index = true)
    public String key;
    @QuerySqlField(index = true, descending = true)
    public long timestamp;

    @Override
    public String toString() {
        return  "\"key\": " + key +
                ", \"timestamp\": " + timestamp;
    }
}
