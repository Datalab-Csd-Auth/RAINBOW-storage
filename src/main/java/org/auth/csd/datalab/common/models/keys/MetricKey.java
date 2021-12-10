package org.auth.csd.datalab.common.models.keys;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.auth.csd.datalab.common.models.InputJson;

public class MetricKey {

    public MetricKey(InputJson tmp) {
        this.metricID = tmp.metricID;
        this.entityID = tmp.entityID;
    }

    public MetricKey(String metricID, String entityID) {
        this.metricID = metricID;
        this.entityID = entityID;
    }

    @QuerySqlField(index = true)
    public String metricID;
    @QuerySqlField(index = true)
    public String entityID;

    @Override
    public String toString() {
        return "{" +
                " \"metricID\": \"" + metricID + "\"" +
                ", \"entityID\": \"" + entityID + "\"" +
                '}';
    }
}
