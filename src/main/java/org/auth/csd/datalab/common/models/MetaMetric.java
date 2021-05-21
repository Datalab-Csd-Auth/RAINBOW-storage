package org.auth.csd.datalab.common.models;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class MetaMetric {

    public MetaMetric(InputJson tmp) {
        this.entityType = tmp.entityType;
        this.name = tmp.name;
        this.units = tmp.units;
        this.desc = tmp.desc;
        this.groupName = tmp.group;
        this.minVal = tmp.minVal;
        this.maxVal = tmp.maxVal;
        this.higherIsBetter = tmp.higherIsBetter;
    }

    @QuerySqlField
    public String entityType;
    @QuerySqlField
    public String name;
    @QuerySqlField
    public String units;
    @QuerySqlField
    public String desc;
    @QuerySqlField
    public String groupName;
    @QuerySqlField
    public double minVal;
    @QuerySqlField
    public double maxVal;
    @QuerySqlField
    public boolean higherIsBetter;

    @Override
    public String toString() {
        return "{" +
                "\"entityType\": \"" + entityType + "\"" +
                ", \"name\": \"" + name + "\"" +
                ", \"units\": \"" + units + "\"" +
                ", \"desc\": \"" + desc + "\"" +
                ", \"group\": \"" + groupName + "\"" +
                ", \"minVal\": " + minVal +
                ", \"maxVal\": " + maxVal +
                ", \"higherIsBetter\": " + higherIsBetter +
                '}';
    }
}
