package org.auth.csd.datalab.common.models.values;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.auth.csd.datalab.common.models.InputJson;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

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

    public MetaMetric(String entityType, String name, String units, String desc, String groupName, double minVal, double maxVal, boolean higherIsBetter, String podUUID, String podName, String podNamespace, String containerID, String containerName) {
        this.entityType = entityType;
        this.name = name;
        this.units = units;
        this.desc = desc;
        this.groupName = groupName;
        this.minVal = minVal;
        this.maxVal = maxVal;
        this.higherIsBetter = higherIsBetter;
        this.podUUID = podUUID;
        this.podName = podName;
        this.podNamespace = podNamespace;
        this.containerID = containerID;
        this.containerName = containerName;
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
        return  "\"entityType\": \"" + entityType + "\"" +
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
                "}";
    }

    public String toString(Set<String> filter) {
        System.out.println(filter);
        Set<String> pod = filter.stream().filter(k -> k.contains("pod")).collect(Collectors.toSet());
        Set<String> container = filter.stream().filter(k -> k.contains("container")).collect(Collectors.toSet());
        Set<String> newFilter = new HashSet<>(filter);
        newFilter.removeIf(k -> k.contains("pod") || k.contains("container"));
        StringBuilder result = new StringBuilder();
        newFilter.forEach(k -> {
            try {
                Object val = this.getClass().getField(k).get(this);
                result.append("\"").append(k).append("\": ");
                boolean b = !k.equals("minVal") && !k.equals("maxVal") && !k.equals("higherIsBetter");
                if(b)
                    result.append("\"");
                result.append(val);
                if(b)
                    result.append("\"");
                result.append(",");
            } catch (NoSuchFieldException | IllegalAccessException ignored){}
        });
        if(!pod.isEmpty()){
            result.append("\"pod\": {");
            beautify(pod, result);
        }
        if(!container.isEmpty()){
            result.append("\"container\": {");
            beautify(container, result);
        }
        if(result.toString().endsWith(","))
            result.deleteCharAt(result.length() - 1);
        return result.toString();
    }

    private void beautify(Set<String> container, StringBuilder result) {
        container.forEach(k -> {
            try {
                Object val = this.getClass().getField(k).get(this);
                result.append("\"").append(k).append("\": \"").append(val).append("\",");
            } catch (NoSuchFieldException | IllegalAccessException ignored){}
        });
        result.deleteCharAt(result.length() - 1);
        result.append("},");
    }
}
