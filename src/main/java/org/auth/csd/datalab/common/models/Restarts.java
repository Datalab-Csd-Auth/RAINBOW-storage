package org.auth.csd.datalab.common.models;

import java.util.ArrayList;
import java.util.List;

public class Restarts {
    public List<Long> restartTimes;
    public List<Long> failTimes;
    public long startTime;
    public long lastReplication;

    public Restarts(long startTime) {
        this.startTime = startTime;
        restartTimes = new ArrayList<>();
        failTimes = new ArrayList<>();
        lastReplication = 0L;
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
}
