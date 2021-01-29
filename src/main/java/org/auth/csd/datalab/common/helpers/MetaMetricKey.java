package org.auth.csd.datalab.common.helpers;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class MetaMetricKey {

    public MetaMetricKey(InputJson tmp) {
        this.entityID = tmp.entityID;
        this.metricID = tmp.metricID;
    }

    @QuerySqlField(index = true)
    public String entityID;
    @QuerySqlField(index = true)
    public String metricID;

    @Override
    public String toString() {
        return "{" +
                "\"entityID\": \"" + entityID + "\"" +
                ", \"metricID\": \"" + metricID + "\"" +
                '}';
    }
}
