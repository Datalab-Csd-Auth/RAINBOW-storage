package org.auth.csd.datalab.common.models;

import java.util.ArrayList;
import java.util.List;

public class Restarts {
    public int restarts;
    public List<Long> startTimes;

    public Restarts() {
        restarts = 0;
        startTimes = new ArrayList<>();
    }

    public Restarts(long timestamp) {
        restarts = 0;
        startTimes = new ArrayList<>();
        startTimes.add(timestamp);
    }

    public void addRestart(long timestamp) {
        restarts += 1;
        startTimes.add(timestamp);
    }
}
