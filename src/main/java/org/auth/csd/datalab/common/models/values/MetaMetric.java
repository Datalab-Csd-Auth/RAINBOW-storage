package org.auth.csd.datalab.common.models.values;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.auth.csd.datalab.common.models.InputJson;

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
        this.podUUID = (tmp.pod != null) ? tmp.pod.uuid : "null";
        this.podName = (tmp.pod != null) ? tmp.pod.name : "null";
        this.podNamespace = (tmp.pod != null) ? tmp.pod.namespace: "null";
        this.containerID = (tmp.container != null) ? tmp.container.id : "null";
        this.containerName = (tmp.container != null) ? tmp.container.name : "null";
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
    @QuerySqlField
    public String podUUID;
    @QuerySqlField
    public String podName;
    @QuerySqlField
    public String podNamespace;
    @QuerySqlField
    public String containerID;
    @QuerySqlField
    public String containerName;

    @Override
    public String toString() {
        return "{" +
                " \"entityType\": \"" + entityType + "\"" +
                ", \"name\": \"" + name + "\"" +
                ", \"units\": \"" + units + "\"" +
                ", \"desc\": \"" + desc + "\"" +
                ", \"group\": \"" + groupName + "\"" +
                ", \"minVal\": " + minVal +
                ", \"maxVal\": " + maxVal +
                ", \"higherIsBetter\": " + higherIsBetter +
                ", \"pod\": {" +
                    " \"uuid\": \"" + podUUID + "\"" +
                    ", \"name\": \"" + podName + "\"" +
                    ", \"namespace\": \"" + podNamespace + "\"" +
                "}" +
                ", \"container\": {" +
                    " \"id\": \"" + containerID + "\"" +
                    ", \"name\": \"" + containerName + "\"" +
                "}" +
                "}";
    }
}
