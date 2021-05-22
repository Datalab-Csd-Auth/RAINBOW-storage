package org.auth.csd.datalab.common.models;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Metric {

    public Metric(Double val) {
        this.val = val;
    }

    @QuerySqlField
    public Double val;

    @Override
    public String toString() {
        return "{" +
                " \"value\": \"" + val + "\"" +
                '}';
    }
}
