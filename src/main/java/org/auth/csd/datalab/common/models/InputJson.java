package org.auth.csd.datalab.common.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class InputJson {

    public InputJson(@JsonProperty(value= "entityID", required = true)String entityID,
                     @JsonProperty(value= "metricID", required = true)String metricID,
                     @JsonProperty(value= "val", required = true)double val,
                     @JsonProperty(value= "timestamp", required = true)long timestamp) {
        this.entityID = entityID;
        this.metricID = metricID;
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
    public Pod pod;
    public Container container;

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
                ", " + pod +
                ", " + container +
                '}';
    }

    public static class Pod {
        public Pod() {
            //Json initialized
        }
        public String uuid;
        public String namespace;
        public String name;
        @Override
        public String toString() {
            return "\"pod\": {" +
                    " \"uuid\"= \"" + uuid + "\"" +
                    ", \"namespace\"= \"" + namespace + "\"" +
                    ", \"name\"= \"" + name + "\"" +
                    '}';
        }
    }
    public static class Container {
        public Container() {
            //Json initialized
        }
        public String id;
        public String name;
        @Override
        public String toString() {
            return "\"container\": {" +
                    " \"id\"= \"" + id + "\"" +
                    ", \"name\"= \"" + name + "\"" +
                    '}';
        }
    }
}
