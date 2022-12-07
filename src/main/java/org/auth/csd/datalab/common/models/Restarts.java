package org.auth.csd.datalab.common.models;

import java.util.ArrayList;
import java.util.List;

public class Restarts {
    public List<Long> restartTimes;
    public List<Long> failTimes;
    public long startTime;
    public long lastReplication;
    public List<String> replicaTo;
    public List<String> replicaFrom;

    public Restarts(long startTime) {
        this.startTime = startTime;
        restartTimes = new ArrayList<>();
        failTimes = new ArrayList<>();
        lastReplication = 0L;
        replicaTo = new ArrayList<>();
        replicaFrom = new ArrayList<>();
    }


    public void addRestart(long timestamp) {
        restartTimes.add(timestamp);
    }

    public void addFail(long timestamp) {
        failTimes.add(timestamp);
    }

    public void addReplication(long timestamp){
        lastReplication = timestamp;
    }

    public void addDestination(String dest){
        replicaTo.add(dest);
    }

    public void removeDestination(String dest){
        replicaTo.remove(dest);
    }

    public void clearDestination(){replicaTo.clear();}

    public void addSource(String source){
        replicaFrom.add(source);
    }

    public void removeSource(String source){
        replicaFrom.remove(source);
    }

    public void clearSource(){replicaFrom.clear();}
}
