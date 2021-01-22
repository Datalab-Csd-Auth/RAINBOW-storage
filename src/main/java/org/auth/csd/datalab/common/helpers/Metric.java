package org.auth.csd.datalab.common.helpers;

public class Metric {

    public String entityID;
    public String entityType;
    public String metricID;
    public String name;
    public String units;
    public String desc;
    public String group;
    public int minVal;
    public int maxVal;
    public boolean higherIsBetter;
    public int val;
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
