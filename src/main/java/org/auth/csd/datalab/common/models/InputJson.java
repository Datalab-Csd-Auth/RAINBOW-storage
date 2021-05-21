package org.auth.csd.datalab.common.models;

public class InputJson {

    public InputJson() {
        this.entityID = entityID;
        this.entityType = entityType;
        this.metricID = metricID;
        this.name = name;
        this.units = units;
        this.desc = desc;
        this.group = group;
        this.minVal = minVal;
        this.maxVal = maxVal;
        this.higherIsBetter = higherIsBetter;
        this.val = val;
        this.timestamp = timestamp;
    }

    public String entityID;
    public String entityType;
    public String metricID;
    public String name;
    public String units;
    public String desc;
    public String group;
    public double minVal;
    public double maxVal;
    public boolean higherIsBetter;
    public double val;
    public long timestamp;

    @Override
    public String toString() {
        return "{" +
                "\"entityID\": \"" + entityID + "\"" +
                ", \"entityType\": \"" + entityType + "\"" +
                ", \"metricID\": \"" + metricID + "\"" +
                ", \"name\": \"" + name + "\"" +
                ", \"units\": \"" + units + "\"" +
                ", \"desc\": \"" + desc + "\"" +
                ", \"group\": \"" + group + "\"" +
                ", \"minVal\": " + minVal +
                ", \"maxVal\": " + maxVal +
                ", \"higherIsBetter\": " + higherIsBetter +
                ", \"val\": " + val +
                ", \"timestamp\": " + timestamp +
                '}';
    }
}
